# Data Notes

Reference notes on the source data this pipeline ingests, the file
conventions, the value semantics, and how to interpret what lands in S3.

## Source collection

**`OPERA_L2_RTC-S1_V1`** (queried via CMR/earthaccess).

- **L2** — Level-2 product, derived from Sentinel-1 L1 GRD.
- **RTC** — Radiometrically Terrain Corrected. Hillshading and
  foreshortening effects are removed, so a coastal/island pixel reads
  the same regardless of local terrain. This matters for polar
  latitudes where mountainous coastlines (Greenland, Svalbard) would
  otherwise contaminate the signal.
- **S1** — Sentinel-1 SAR (C-band, ~5.4 GHz).
- **V1** — Validated v1, OPERA's stable global release.

Each CMR granule = one Sentinel-1 IW frame = ~50 km × 50 km, delivered
as several files (see "Per-granule files" below).

## Polarizations

Each polarization label is two letters: **first** is what the satellite
transmits, **second** is what it receives.

| Code | Transmit | Receive | Type |
|---|---|---|---|
| `VV` | Vertical | Vertical | co-pol |
| `VH` | Vertical | Horizontal | cross-pol |
| `HH` | Horizontal | Horizontal | co-pol |
| `HV` | Horizontal | Vertical | cross-pol |

**Sentinel-1 IW mode** transmits vertically and receives both V and H
simultaneously, so every granule contains exactly **`VV` + `VH`**.
`HH` and `HV` don't exist for standard S1 IW.

### Choosing between VV and VH

| Polarization | Signal strength | Best for |
|---|---|---|
| **VV** (co-pol) | Strong | Surface roughness, water surfaces, ocean wind/waves, agricultural fields, urban areas |
| **VH** (cross-pol) | Weaker, more discriminative | Volume scattering — distinguishing **sea ice types**, vegetation, snow, forests |

**Why this pipeline uses `*VH*.tif`:**

- VH cross-pol responds to volume scattering — the radar pulse bounces
  around inside the ice's internal structure before returning.
- This separates **first-year ice** (smooth, low VH) from **multi-year
  ice** (deformed, rougher, higher VH) much more cleanly than VV.
- It also separates ice from open water cleanly: open water gives very
  low VH (calm surface → little cross-pol return), sea ice noticeably
  higher.
- VV can confuse calm water with new smooth ice; VH doesn't.

**For full ice-type classification** (operational sea ice work usually
wants this) the typical approach is to use BOTH VV and VH and compute
the **VH/VV ratio** — that ratio is a powerful discriminator. We could
add VV ingestion as a parallel collection (e.g. `frozon-rtc-s1-vv-daily`)
when downstream consumers want that channel.

## Per-granule files

A single OPERA RTC-S1 granule URL lists three TIFFs that share the same
granule ID prefix:

```
OPERA_L2_RTC-S1_T<tile>_<datetime>_<datetime>_S1A_30_v1.0_VV.tif    ← backscatter
OPERA_L2_RTC-S1_T<tile>_<datetime>_<datetime>_S1A_30_v1.0_VH.tif    ← backscatter
OPERA_L2_RTC-S1_T<tile>_<datetime>_<datetime>_S1A_30_v1.0_mask.tif  ← quality flags
```

The `*_mask.tif` file is **not a backscatter measurement** — it flags
pixels affected by layover, radar shadow, or other geometric artifacts.
This pipeline ignores it; only `*VH*.tif` is ingested.

## Backscatter calibration convention

Three standard ways to normalize the raw backscatter measurement:

| Convention | Symbol | Normalized by |
|---|---|---|
| Beta-naught | β⁰ | Slant-range plane area (raw-ish, pre-terrain-correction) |
| Sigma-naught | σ⁰ | Ground-plane area (assumes flat earth at sea level) |
| Gamma-naught | γ⁰ | Area perpendicular to radar line-of-sight |

**This pipeline produces σ⁰** (sigma-naught) by applying the per-pixel
sigmaNought LUT from the SAFE bundle to the raw DN band (formula:
`σ⁰ = DN² / k²`, then `10 * log10` to convert to decibels). The β⁰
LUT is also wired up in code if a future consumer needs it.

### Pixel values

The COGs this pipeline produces store **σ⁰ in decibels (dB), Float32**.
NoData is `NaN`.

Storing in dB matches NSIDC / Polar View / standard sea-ice products
and the project's sibling `s1_calibrate.py` Earth Engine workflow.
Downstream MMGIS / TiTiler can rescale directly without an
`expression=10*log10(b1)` layer-side transformation.

**Typical HH σ⁰ ranges** (rough rules of thumb — actual values vary
with season, surface state, frequency, incidence angle):

| Surface | σ⁰ dB | (equivalent linear σ⁰) |
|---|---|---|
| Open water (calm) | -30 to -20 dB | 0.001 – 0.01 |
| Open water (rough) | -20 to -12 dB | 0.01 – 0.06 |
| First-year ice | -18 to -8 dB | 0.016 – 0.16 |
| Multi-year ice | -12 to -3 dB | 0.06 – 0.5 |
| Land / rough surfaces | -10 to +5 dB | 0.1 – 3 |

### Display ranges

For MMGIS / TiTiler / QGIS — directly rescale the dB values:

| Use case | vmin | vmax |
|---|---|---|
| Sea-ice contrast (recommended) | -30 | +10 |
| Tight ocean focus | -25 | -5 |
| Show very bright targets | -30 | +15 |

No log transformation needed at the layer — `rescale=-30,10` is enough.

### Converting to other conventions

dB → linear (in case a downstream wants linear):

```python
import numpy as np
sigma0_linear = 10 ** (sigma0_db / 10)
# NaN-safe; sigma0_linear is dimensionless, > 0 where valid.
```

σ⁰ → γ⁰ (incidence-angle correction):

```
γ⁰_linear = σ⁰_linear / cos(θ)
γ⁰_dB     = σ⁰_dB − 10·log10(cos(θ))
```

where **θ** is the local incidence angle. Sentinel-1 GRD ships an
incidence-angle annotation XML alongside the calibration LUTs, but
we don't propagate it into the COG — for sea-ice work on flat ocean
the incidence effect is small enough to ignore at our display scale.

## Coverage expectations

Sentinel-1's Arctic acquisition is **not uniform**. ESA's [acquisition
scenario](https://sentinels.copernicus.eu/web/sentinel/missions/sentinel-1/observation-scenario)
prioritizes the **Eurasian Arctic** (Barents/Kara/Laptev seas) for
operational sea-ice monitoring; the **Canadian / Pacific Arctic** gets
observed on a much sparser schedule.

Practical implication for our daily mosaics:

- A single day's COG will show ~30-50 visible swaths, biased heavily
  toward the Eurasian side, with the Western Arctic mostly empty.
- This is **the real underlying acquisition pattern**, not a pipeline
  bug. Compare against NASA Worldview's `OPERA_RTC_S1` layer for the
  same date to confirm.
- The Zarr time series fills in coverage over multiple days: a 7-day
  Zarr will have much more spatially-complete Arctic coverage than any
  single COG.

## Spatial reference

All output COGs and the Zarr are in **EPSG:3413** — NSIDC Sea Ice Polar
Stereographic North. North pole at (0,0), units in meters. The full
Arctic disk is roughly bounded by `±3,300,000 m` in both axes.

`gdalwarp` reprojects each per-granule UTM zone onto the EPSG:3413 grid
during mosaicking. Resolution: 30 m.
