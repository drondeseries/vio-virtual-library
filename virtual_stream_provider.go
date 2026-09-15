package main

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	pb "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const virtualStreamProviderID = "com.drondeseries.vio-virtual-library"

// virtualStreamProvider exposes the provider-neutral SDK contract. The host
// asks for candidates at playback time; provider URLs are deliberately not
// persisted in the catalog.
type virtualStreamProvider struct{ resolver *manifestStreamResolver }

func (s *virtualStreamProvider) ListVirtualStreamProfiles(_ context.Context, req *pb.ListVirtualStreamProfilesRequest) (*pb.ListVirtualStreamProfilesResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("profile request is required")
	}
	mediaType := strings.ToLower(strings.TrimSpace(req.GetMediaType()))
	if mediaType == "episode" {
		mediaType = "series"
	}
	if mediaType != "movie" && mediaType != "series" {
		return nil, fmt.Errorf("unsupported media type %q", mediaType)
	}
	configured := s.resolver.GetConfiguredVariants("virtual://" + mediaType + "/tt0")
	response := &pb.ListVirtualStreamProfilesResponse{}
	for _, variant := range configured {
		parsed, err := url.Parse(variant.VirtualURI)
		if err != nil {
			continue
		}
		response.Profiles = append(response.Profiles, &pb.VirtualStreamProfile{
			Label: variant.Label, Resolution: variant.Resolution,
			VideoCodec: variant.CodecVideo, AudioCodec: variant.CodecAudio,
			HdrFormat:  variant.HDR,
			AllResults: strings.EqualFold(parsed.Query().Get("results"), "all"),
		})
	}
	return response, nil
}

func (s *virtualStreamProvider) ResolveVirtualStream(ctx context.Context, req *pb.ResolveVirtualStreamRequest) (*pb.ResolveVirtualStreamResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("resolve request is required")
	}
	path, err := virtualPathForResolveRequest(req)
	if err != nil {
		return nil, err
	}
	forceRefresh := false
	if metadata := req.GetMetadata(); metadata != nil {
		if value, ok := metadata.AsMap()["force_refresh"]; ok {
			switch typed := value.(type) {
			case bool:
				forceRefresh = typed
			case string:
				forceRefresh = strings.EqualFold(strings.TrimSpace(typed), "true")
			}
		}
	}
	var candidates []StreamCandidate
	if forceRefresh {
		// A forced refresh with no excluded candidates is a genuine re-list:
		// the host is recovering from a dead stream (relay 502) and needs a
		// fresh provider answer, not the cached candidates that just failed.
		// When candidates are excluded the host is walking the failover list
		// inside one playback start, so the freshServeFloor still applies to
		// keep that walk on a single provider round-trip.
		if len(req.GetExcludedCandidateIds()) == 0 {
			candidates, _, _, err = s.resolver.GetCandidatesFreshUnbounded(ctx, path)
		} else {
			candidates, _, _, err = s.resolver.GetCandidatesFresh(ctx, path)
		}
	} else {
		candidates, _, _, err = s.resolver.GetCandidates(ctx, path)
	}
	if err != nil {
		var unreleased *unreleasedError
		if errors.As(err, &unreleased) {
			return &pb.ResolveVirtualStreamResponse{Result: &pb.VirtualStreamResult{
				ProviderId:   virtualStreamProviderID,
				Availability: &pb.VirtualStreamAvailability{State: pb.VirtualStreamAvailabilityState_VIRTUAL_STREAM_AVAILABILITY_STATE_UNAVAILABLE, Message: unreleased.message},
				Error:        &pb.VirtualStreamError{Code: pb.VirtualStreamErrorCode_VIRTUAL_STREAM_ERROR_CODE_NOT_FOUND, Message: unreleased.message, Retryable: true},
			}}, nil
		}
		return &pb.ResolveVirtualStreamResponse{Result: &pb.VirtualStreamResult{
			ProviderId:   virtualStreamProviderID,
			Availability: &pb.VirtualStreamAvailability{State: pb.VirtualStreamAvailabilityState_VIRTUAL_STREAM_AVAILABILITY_STATE_UNAVAILABLE, Message: err.Error()},
			Error:        &pb.VirtualStreamError{Code: pb.VirtualStreamErrorCode_VIRTUAL_STREAM_ERROR_CODE_PROVIDER_FAILURE, Message: err.Error(), Retryable: true},
		}}, nil
	}
	candidates = s.resolver.SelectCandidates(path, candidates)
	if len(req.GetExcludedCandidateIds()) > 0 {
		excluded := make(map[string]struct{}, len(req.GetExcludedCandidateIds()))
		for _, id := range req.GetExcludedCandidateIds() {
			if trimmed := strings.TrimSpace(id); trimmed != "" {
				excluded[trimmed] = struct{}{}
			}
		}
		filtered := make([]StreamCandidate, 0, len(candidates))
		for _, c := range candidates {
			if _, drop := excluded[candidateVariantID(c)]; !drop {
				filtered = append(filtered, c)
			}
		}
		candidates = filtered
	}
	if preferredID := strings.TrimSpace(req.GetPreferredCandidateId()); preferredID != "" && len(candidates) > 1 {
		for i, c := range candidates {
			if candidateVariantID(c) == preferredID {
				if i > 0 {
					reordered := make([]StreamCandidate, 0, len(candidates))
					reordered = append(reordered, c)
					reordered = append(reordered, candidates[:i]...)
					reordered = append(reordered, candidates[i+1:]...)
					candidates = reordered
				}
				break
			}
		}
	}
	quality := s.resolver.qualityConfig()
	resultMetadata, metadataErr := structpb.NewStruct(map[string]any{"cache_ttl_seconds": float64(s.resolver.cacheTTLSeconds())})
	if metadataErr != nil {
		return nil, fmt.Errorf("build stream result metadata: %w", metadataErr)
	}
	result := &pb.VirtualStreamResult{ResultId: path, ProviderId: virtualStreamProviderID, Metadata: resultMetadata, Availability: &pb.VirtualStreamAvailability{State: pb.VirtualStreamAvailabilityState_VIRTUAL_STREAM_AVAILABILITY_STATE_AVAILABLE}}
	resultsAll := false
	if parsedPath, err := url.Parse(path); err == nil {
		resultsAll = strings.EqualFold(parsedPath.Query().Get("results"), "all")
	}
	if !resultsAll && req.GetMetadata() != nil {
		if raw, ok := req.GetMetadata().AsMap()["results"]; ok {
			if s, ok := raw.(string); ok {
				resultsAll = strings.EqualFold(strings.TrimSpace(s), "all")
			}
		}
	}
	for rank, candidate := range candidates {
		candidateMetadata := map[string]any{}
		if quality.SingleStreamWithFailover {
			if resultsAll {
				candidateMetadata["visible"] = true
			} else {
				candidateMetadata["visible"] = rank == 0
			}
		}
		if displayName := candidateDisplayName(candidate); displayName != "" {
			candidateMetadata["display_name"] = displayName
		}
		if sourceType := strings.TrimSpace(candidate.SourceType); sourceType != "" {
			candidateMetadata["source_type"] = sourceType
		}
		if candidate.SourceConfirmed {
			// Advisory hint for hosts and diagnostics. The durable preference
			// itself is the rank ordering applied by the resolver; the server
			// reads ranks and ignores unknown metadata keys today.
			candidateMetadata["source_confirmed"] = true
		}
		metadata, metadataErr := structpb.NewStruct(candidateMetadata)
		if metadataErr != nil {
			return nil, fmt.Errorf("build candidate metadata: %w", metadataErr)
		}
		candProto := &pb.VirtualStreamCandidate{
			CandidateId: candidateVariantID(candidate), ProviderId: virtualStreamProviderID, TemporaryUri: candidate.URL,
			Rank: int32(rank + 1), Resolution: &pb.VirtualStreamResolution{Label: candidate.Resolution},
			VideoCodec: candidate.CodecVideo, AudioCodec: candidate.CodecAudio,
			Hdr:           &pb.VirtualStreamHDR{IsHdr: candidate.HDR != "", Format: candidate.HDR, HasDolbyVision: strings.EqualFold(candidate.HDR, "dv")},
			FileSizeBytes: candidate.FileSize, Container: candidate.Container,
			AudioLanguages: candidate.AudioLanguages, SubtitleLanguages: candidate.SubtitleLanguages,
			RequestHeaders: candidate.RequestHeaders,
			HasAtmos:       candidate.HasAtmos,
			QualityScore:   int32(candidate.QualityScore),
			Metadata:       metadata,
			Availability:   &pb.VirtualStreamAvailability{State: pb.VirtualStreamAvailabilityState_VIRTUAL_STREAM_AVAILABILITY_STATE_AVAILABLE},
		}
		if !candidate.ExpiresAt.IsZero() {
			candProto.ExpiresAt = timestamppb.New(candidate.ExpiresAt)
		}
		result.Candidates = append(result.Candidates, candProto)
	}
	if len(result.Candidates) == 0 {
		result.Availability = &pb.VirtualStreamAvailability{State: pb.VirtualStreamAvailabilityState_VIRTUAL_STREAM_AVAILABILITY_STATE_UNAVAILABLE, Message: "provider returned no streams"}
		result.Error = &pb.VirtualStreamError{Code: pb.VirtualStreamErrorCode_VIRTUAL_STREAM_ERROR_CODE_NOT_FOUND, Message: "provider returned no streams", Retryable: true}
	}
	return &pb.ResolveVirtualStreamResponse{Result: result}, nil
}

// virtualPathForResolveRequest preserves playback selection metadata while
// binding it to the request's media identity. This prevents a caller from
// resolving an unrelated virtual URI by changing only metadata.
func virtualPathForResolveRequest(req *pb.ResolveVirtualStreamRequest) (string, error) {
	canonical, err := virtualPathForRequest(req)
	if err != nil {
		return "", err
	}
	metadata := req.GetMetadata()
	if metadata == nil {
		return canonical, nil
	}
	raw, present := metadata.AsMap()["virtual_uri"]
	if !present {
		return canonical, nil
	}
	uri, ok := raw.(string)
	if !ok || strings.TrimSpace(uri) == "" {
		return "", fmt.Errorf("metadata virtual_uri must be a non-empty string")
	}
	uri = strings.TrimSpace(uri)
	mediaType, mediaID, err := parseVirtualPath(uri)
	if err != nil {
		return "", fmt.Errorf("invalid metadata virtual_uri: %w", err)
	}
	expectedType, expectedID, err := parseVirtualPath(canonical)
	if err != nil {
		return "", err
	}
	if mediaType != expectedType || !strings.EqualFold(mediaID, expectedID) {
		return "", fmt.Errorf("metadata virtual_uri does not match requested media")
	}
	return uri, nil
}

func virtualPathForRequest(req *pb.ResolveVirtualStreamRequest) (string, error) {
	ids := req.GetExternalIds()
	id := strings.TrimSpace(ids["imdb"])
	if id == "" {
		if tvdb := strings.TrimSpace(ids["tvdb"]); tvdb != "" {
			id = "tvdb:" + tvdb
		} else if tmdb := strings.TrimSpace(ids["tmdb"]); tmdb != "" {
			id = "tmdb:" + tmdb
		}
	}
	if id == "" {
		return "", fmt.Errorf("an IMDb, TVDB, or TMDB external ID is required")
	}
	typ := strings.ToLower(strings.TrimSpace(req.GetMediaType()))
	if typ == "episode" {
		if req.GetSeasonNumber() <= 0 || req.GetEpisodeNumber() <= 0 {
			return "", fmt.Errorf("season and episode are required")
		}
		typ = "series"
		return fmt.Sprintf("virtual://series/%s/%d/%d", id, req.GetSeasonNumber(), req.GetEpisodeNumber()), nil
	}
	if typ != "movie" && typ != "series" {
		return "", fmt.Errorf("unsupported media type %q", typ)
	}
	return "virtual://" + typ + "/" + id, nil
}

var _ pb.VirtualStreamProviderServer = (*virtualStreamProvider)(nil)
