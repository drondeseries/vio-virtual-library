package main

import (
	"testing"

	"github.com/Silo-Server/silo-plugin-sdk/pkg/pluginsdk/runtimehost"
)

func TestMovieVirtualURIReturnsEmptyForSeries(t *testing.T) {
	if got := movieVirtualURI("movie", "tt123:4"); got != "virtual://movie/tt123/4" {
		t.Fatalf("movie URI = %q, want virtual://movie/tt123/4", got)
	}
	if got := movieVirtualURI("series", "tt123"); got != "" {
		t.Fatalf("series URI = %q, want empty series-level URI", got)
	}
}

func TestNewVioLibraryDefaultsMissingIDs(t *testing.T) {
	host := &runtimehost.Client{}
	resolver := &manifestStreamResolver{}

	t.Run("zero IDs default to 1 and 2", func(t *testing.T) {
		lib, err := newVioLibrary(host, 0, 0, resolver)
		if err != nil {
			t.Fatalf("newVioLibrary(0,0): %v", err)
		}
		if lib.movieLibraryID != 1 {
			t.Errorf("movieLibraryID = %d, want 1", lib.movieLibraryID)
		}
		if lib.seriesLibraryID != 2 {
			t.Errorf("seriesLibraryID = %d, want 2", lib.seriesLibraryID)
		}
	})

	t.Run("negative IDs default to 1 and 2", func(t *testing.T) {
		lib, err := newVioLibrary(host, -5, -1, resolver)
		if err != nil {
			t.Fatalf("newVioLibrary(-5,-1): %v", err)
		}
		if lib.movieLibraryID != 1 {
			t.Errorf("movieLibraryID = %d, want 1", lib.movieLibraryID)
		}
		if lib.seriesLibraryID != 2 {
			t.Errorf("seriesLibraryID = %d, want 2", lib.seriesLibraryID)
		}
	})

	t.Run("explicit IDs are preserved", func(t *testing.T) {
		lib, err := newVioLibrary(host, 42, 99, resolver)
		if err != nil {
			t.Fatalf("newVioLibrary(42,99): %v", err)
		}
		if lib.movieLibraryID != 42 {
			t.Errorf("movieLibraryID = %d, want 42", lib.movieLibraryID)
		}
		if lib.seriesLibraryID != 99 {
			t.Errorf("seriesLibraryID = %d, want 99", lib.seriesLibraryID)
		}
	})

	t.Run("nil host returns error regardless of IDs", func(t *testing.T) {
		_, err := newVioLibrary(nil, 1, 2, resolver)
		if err == nil {
			t.Fatal("expected error for nil host, got nil")
		}
	})
}

func TestValidateLibraryIDsNilHostIsNoop(t *testing.T) {
	// A nil host skips validation gracefully — this happens when Configure
	// is called before the plugin runtime has wired the host client.
	if err := validateLibraryIDs(nil, 1, 2); err != nil {
		t.Fatalf("validateLibraryIDs(nil): unexpected error: %v", err)
	}
}
