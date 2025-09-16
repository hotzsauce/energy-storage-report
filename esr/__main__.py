import argparse
from esr.webdriver import download_date_range

def main():
    parser = argparse.ArgumentParser(
        description="A small CLI tool for pulling CAISO's ESR data"
    )

    parser.add_argument(
        "start",
        help="The first date in the date range",
    )
    parser.add_argument(
        "-e",
        "--end",
        default="",
        help="The final date in the date range"
    )
    parser.add_argument(
        "-o",
        "--output",
        default="data",
        help="The name of the directory into which the data is written"
    )
    parser.add_argument(
        "-b",
        "--hybrid",
        default=False,
        help="Whether or not to grab the hybrid data",
    )

    args = parser.parse_args()
    download_date_range(args.start, args.end, args.output, args.hybrid)

raise SystemExit(main())
