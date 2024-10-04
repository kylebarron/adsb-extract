from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


def count_total_num_coords(data_paths: list[Path]):
    """Count total number of coordinates"""
    num_coords = 0
    for data_path in data_paths:
        with open(data_path) as f:
            trace = json.load(f)

        num_coords += len(trace["trace"])

    return num_coords


def build_arrow_table(data_paths: list[Path], total_num_coords: int):
    num_rows = len(data_paths)
    coords = np.zeros((total_num_coords, 3), dtype=np.float64)
    coord_offsets = np.zeros(num_rows + 1, dtype=np.int32)

    # millisecond-based timestamps
    timestamps = np.zeros(total_num_coords, dtype=np.int64)

    ground_speeds = np.zeros(total_num_coords, dtype=np.float32)
    headings = np.zeros(total_num_coords, dtype=np.float32)
    vertical_speeds = np.zeros(total_num_coords, dtype=np.float32)

    icao_ids: list[str] = []

    coord_offset = 0
    for trace_idx, data_path in enumerate(data_paths):
        with open(data_path) as f:
            trace = json.load(f)

        icao_ids.append(trace["icao"])
        start_timestamp = trace["timestamp"]
        for record in trace["trace"]:
            # add start_timestamp and convert to ms
            current_timestamp = (start_timestamp + record[0]) * 1000
            timestamps[coord_offset] = current_timestamp

            lat = record[1]
            lon = record[2]
            alt = record[3]
            ground_speed = record[4]
            heading = record[5]
            vertical_speed = record[7]

            coords[coord_offset, 0] = lon
            coords[coord_offset, 1] = lat
            if alt == "ground":
                coords[coord_offset, 2] = 0
            else:
                coords[coord_offset, 2] = alt

            ground_speeds[coord_offset] = ground_speed
            headings[coord_offset] = heading
            vertical_speeds[coord_offset] = vertical_speed

            coord_offset += 1

        coord_offsets[trace_idx + 1] = coord_offset

    geometry = pa.ListArray.from_arrays(
        coord_offsets, pa.FixedSizeListArray.from_arrays(coords.ravel("C"), 3)
    )
    pa_timestamps = pa.ListArray.from_arrays(
        coord_offsets, pa.array(timestamps, type=pa.timestamp("ms"))
    )

    pa_ground_speeds = pa.ListArray.from_arrays(coord_offsets, ground_speeds)
    pa_headings = pa.ListArray.from_arrays(coord_offsets, headings)
    pa_vertical_speeds = pa.ListArray.from_arrays(coord_offsets, vertical_speeds)
    pa_icao_ids = pa.array(icao_ids)

    geometry_field = pa.field(
        "geometry",
        geometry.type,
        metadata={b"ARROW:extension:name": b"geoarrow.linestring"},
    )
    fields = [
        geometry_field,
        pa.field("timestamp", pa_timestamps.type),
        pa.field("ground_speed", pa_ground_speeds.type),
        pa.field("heading", pa_headings.type),
        pa.field("vertical_speed", pa_vertical_speeds.type),
        pa.field("icao_id", pa_icao_ids.type),
    ]
    schema = pa.schema(fields)
    table = pa.Table.from_arrays(
        [
            geometry,
            pa_timestamps,
            pa_ground_speeds,
            pa_headings,
            pa_vertical_speeds,
            pa_icao_ids,
        ],
        schema=schema,
    )
    pq.write_table(table, "data/2024-10-03_traces.parquet", compression="zstd")


def main():
    data_paths = list(Path("data/filter_bbox").iterdir())
    total_num_coords = count_total_num_coords(data_paths)
    build_arrow_table(data_paths, total_num_coords)
