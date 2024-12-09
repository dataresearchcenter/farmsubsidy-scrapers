"""
CLI commands
"""

import importlib
import pathlib

import click

DEFAULT_YEAR = 2022
DATA_DIR = "data"


@click.command()
@click.argument("country")
@click.argument("year")
def cli(country=None, year=DEFAULT_YEAR):
    """Download farm subsidy data for specified country and year."""
    print(f"Downloading data for {country} in {year}")

    try:
        # Dynamically import the country module
        country_module = importlib.import_module(f"countries.{country}.run")
        options = {"data_dir": pathlib.Path(DATA_DIR)}

        return country_module.run(year, **options)
    except ImportError:
        print(f"Error: Country module '{country}' not found")
        return 1
    except Exception as e:
        print(f"Error downloading data: {e}")
        return 1


if __name__ == "__main__":
    cli()
