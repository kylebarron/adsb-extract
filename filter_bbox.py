import gzip
import json
from pathlib import Path
from typing import Tuple

filter_bboxes = (
    # LGA
    (-73.893251, 40.767887, -73.853588, 40.786629),
    # JFK
    (-73.825999, 40.620652, -73.746376, 40.668147),
    # EWR
    (-74.195501, 40.670693, -74.152364, 40.709224),
)


def main():
    source_dir = Path(
        "/Users/kyle/Downloads/v2024.04.10-planes-readsb-staging-0/traces"
    )
    output_dir = Path("data/filter_bbox")
    output_dir.mkdir(exist_ok=True, parents=True)

    for trace_dir in sorted(source_dir.iterdir()):
        for trace_file in sorted(trace_dir.iterdir()):
            trace = read_trace_file(trace_file)
            if include_trace(trace, filter_bboxes):
                output_file = output_dir / trace_file.name
                with open(output_file, "w") as outf:
                    json.dump(trace, outf, separators=(",", ":"))


def read_trace_file(file: Path) -> dict:
    with gzip.open(file) as gzipf:
        return json.load(gzipf)


def include_trace(
    trace: dict, filter_bboxes: Tuple[Tuple[float, float, float, float], ...]
) -> bool:
    records = trace["trace"]
    start_lat = records[0][1]
    start_lon = records[0][2]

    end_lat = records[-1][1]
    end_lon = records[-1][2]

    for filter_bbox in filter_bboxes:
        if filter_bbox[0] <= start_lon <= filter_bbox[2]:
            return True

        if filter_bbox[1] <= start_lat <= filter_bbox[3]:
            return True

        if filter_bbox[0] <= end_lon <= filter_bbox[2]:
            return True

        if filter_bbox[1] <= end_lat <= filter_bbox[3]:
            return True

    return False


if __name__ == "__main__":
    main()
