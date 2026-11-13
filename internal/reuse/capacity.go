/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package reuse

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v2"
)

const maxCapacityConfigBytes = 1 << 20 // 1 MiB ceiling on external capacity config file

// ReservationLimit configures the chip capacity of a specific GCE reservation.
type ReservationLimit struct {
	SLOChips int `json:"sloChips" yaml:"sloChips"`
	RawChips int `json:"rawChips,omitempty" yaml:"rawChips,omitempty"`
}

// CellLimit configures the chip capacity of a physical TPU placement cell (cloud.google.com/gke-location-hint)
// and optionally links it 1:1 to a reservation name.
type CellLimit struct {
	Reservation string `json:"reservation,omitempty" yaml:"reservation,omitempty"`
	SLOChips    int    `json:"sloChips,omitempty" yaml:"sloChips,omitempty"`
	RawChips    int    `json:"rawChips,omitempty" yaml:"rawChips,omitempty"`
}

// RawCapacityConfig is the top-level YAML schema for the capacity configuration file.
type RawCapacityConfig struct {
	Reservations map[string]ReservationLimit `json:"reservations,omitempty" yaml:"reservations,omitempty"`
	Cells        map[string]CellLimit        `json:"cells,omitempty" yaml:"cells,omitempty"`
}

// PoolLimit represents the resolved capacity bounds of a canonical CapacityPoolKey.
type PoolLimit struct {
	Key         string
	HasLimit    bool
	SLOCapacity int
	RawCapacity int
}

// CapacityBook resolves (reservation, cell) pairs to canonical capacity pools and ceilings
// strictly from configuration (with zero external GCE API calls).
type CapacityBook struct {
	limits          map[string]PoolLimit
	cellToCanonical map[string]string
	resToCanonical  map[string]string
}

// LoadCapacityConfigFile loads reservation and cell capacity limits from a YAML file path.
// An empty path returns an unconstrained CapacityBook.
func LoadCapacityConfigFile(filePath string) (*CapacityBook, error) {
	rawPath := strings.TrimSpace(filePath)
	if rawPath == "" {
		return NewCapacityBook(RawCapacityConfig{})
	}

	for _, seg := range strings.Split(filepath.ToSlash(rawPath), "/") {
		if seg == ".." {
			return nil, fmt.Errorf("path traversal '..' is not allowed in CAPACITY_CONFIG_PATH %q", rawPath)
		}
	}
	cleanPath := filepath.Clean(rawPath)
	f, err := os.Open(cleanPath)
	if err != nil {
		return nil, fmt.Errorf("opening capacity config file %q: %w", cleanPath, err)
	}
	defer f.Close()

	data, err := io.ReadAll(io.LimitReader(f, maxCapacityConfigBytes+1))
	if err != nil {
		return nil, fmt.Errorf("reading capacity config file %q: %w", cleanPath, err)
	}
	if len(data) > maxCapacityConfigBytes {
		return nil, fmt.Errorf("capacity config file %q exceeds %d bytes", cleanPath, maxCapacityConfigBytes)
	}

	var cfg RawCapacityConfig
	if len(strings.TrimSpace(string(data))) > 0 {
		if err := yaml.UnmarshalStrict(data, &cfg); err != nil {
			return nil, fmt.Errorf("parsing capacity config file %q: %w", cleanPath, err)
		}
	}
	return NewCapacityBook(cfg)
}

// NewCapacityBook validates a RawCapacityConfig and builds a CapacityBook.
func NewCapacityBook(cfg RawCapacityConfig) (*CapacityBook, error) {
	book := &CapacityBook{
		limits:          make(map[string]PoolLimit),
		cellToCanonical: make(map[string]string),
		resToCanonical:  make(map[string]string),
	}

	// 1. Register standalone reservations first.
	for rawResName, rLim := range cfg.Reservations {
		resKey := strings.TrimSpace(rawResName)
		if resKey == "" {
			continue
		}
		if rLim.SLOChips < 0 || rLim.RawChips < 0 {
			return nil, fmt.Errorf("reservation %q has negative chip capacity", resKey)
		}
		slo := rLim.SLOChips
		rawCap := rLim.RawChips
		if rawCap == 0 {
			rawCap = slo
		}
		if slo > 0 && rawCap < slo {
			return nil, fmt.Errorf("reservation %q has rawChips (%d) < sloChips (%d)", resKey, rawCap, slo)
		}
		shortRes := ShortReservationName(resKey)
		canonical := "reservation:" + shortRes
		book.resToCanonical[resKey] = canonical
		book.resToCanonical[shortRes] = canonical
		book.limits[canonical] = PoolLimit{
			Key:         canonical,
			HasLimit:    slo > 0 || rawCap > 0,
			SLOCapacity: slo,
			RawCapacity: rawCap,
		}
	}

	// 2. Register cells (and unify with reservation if cell.Reservation is set).
	for rawCellName, cLim := range cfg.Cells {
		cellName := strings.TrimSpace(rawCellName)
		if cellName == "" {
			continue
		}
		if cLim.SLOChips < 0 || cLim.RawChips < 0 {
			return nil, fmt.Errorf("cell %q has negative chip capacity", cellName)
		}

		linkedRes := strings.TrimSpace(cLim.Reservation)
		var canonical string
		var merged PoolLimit

		if linkedRes != "" {
			shortRes := ShortReservationName(linkedRes)
			if existingCanonical, ok := book.resToCanonical[linkedRes]; ok {
				canonical = existingCanonical
			} else if existingCanonical, ok := book.resToCanonical[shortRes]; ok {
				canonical = existingCanonical
			} else {
				canonical = "cell:" + cellName
			}
			merged = book.limits[canonical]
			book.resToCanonical[linkedRes] = canonical
			book.resToCanonical[shortRes] = canonical
		} else {
			canonical = "cell:" + cellName
			merged = book.limits[canonical]
		}

		slo := maxInt(merged.SLOCapacity, cLim.SLOChips)
		rawCap := maxInt(merged.RawCapacity, cLim.RawChips)
		if rawCap == 0 {
			rawCap = slo
		}
		if slo > 0 && rawCap < slo {
			return nil, fmt.Errorf("cell %q has rawChips (%d) < sloChips (%d)", cellName, rawCap, slo)
		}

		book.cellToCanonical[cellName] = canonical
		book.limits[canonical] = PoolLimit{
			Key:         canonical,
			HasLimit:    slo > 0 || rawCap > 0,
			SLOCapacity: slo,
			RawCapacity: rawCap,
		}
	}

	return book, nil
}

// Resolve maps a (reservation, cell) pair to its canonical PoolLimit.
// Unconfigured pools return HasLimit == false (unconstrained).
func (b *CapacityBook) Resolve(reservation, cell string) PoolLimit {
	res := strings.TrimSpace(reservation)
	c := strings.TrimSpace(cell)

	if b != nil {
		if c != "" {
			if canonical, ok := b.cellToCanonical[c]; ok {
				return b.limits[canonical]
			}
		}
		if res != "" {
			if canonical, ok := b.resToCanonical[res]; ok {
				return b.limits[canonical]
			}
			short := ShortReservationName(res)
			if canonical, ok := b.resToCanonical[short]; ok {
				return b.limits[canonical]
			}
		}
	}

	switch {
	case c != "":
		return PoolLimit{Key: "cell:" + c, HasLimit: false}
	case res != "":
		return PoolLimit{Key: "reservation:" + ShortReservationName(res), HasLimit: false}
	default:
		return PoolLimit{Key: "default", HasLimit: false}
	}
}

// ShortReservationName extracts "<res>" from "projects/<p>/reservations/<res>[/reservationBlocks/...]"
// or "<res>/reservationBlocks/...".
func ShortReservationName(s string) string {
	out := strings.TrimSpace(s)
	const marker = "/reservations/"
	if idx := strings.LastIndex(out, marker); idx != -1 && idx+len(marker) < len(out) {
		out = out[idx+len(marker):]
	}
	return strings.SplitN(out, "/", 2)[0]
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
