"""Convert csv trees to json and optionally validate."""
import pandas as pd
import os
import sys
import click


def check_validity(df):
    """Ensures that csv has valid structure."""

    valid_headers = [
        ['tag', 'description', 'prob_type', 'prob_args'],
        ['tag', 'description', 'prob_type', 'prob_args', 'notes'],
        ['tag', 'description', 'gate_type', 'children'],
        ['tag', 'description', 'gate_type', 'children', 'notes']
    ]

    if list(df.columns) in valid_headers:
        return


    sys.exit('Error: Invalid csv structure.')

def transform(df):
    """Make data type and structure edits to dataframe."""
    if 'prob_args' in df:
        # convert string to list of floats
        df['prob_args'] = df['prob_args'].apply(
            lambda x: [float(i) for i in str(x).split()]
        )

    if 'children' in df:
        # convert string to list of strings
        df['children'] = df['children'].apply(
            lambda x: [i for i in str(x).split()]
        )

    # set up additional columns
    df['state'] = False  # True if event has occured 
    df['t0'] = 0  # time reset


@click.command()
@click.option('--csv', type=click.File('r'), required=True,
              help='Identify csv file to be converted.')
@click.option('--validate', type=click.BOOL, default=False,
              help='Validate json before finishing.')
def main(csv, validate):
    """Convert csv to json."""
    df = pd.read_csv(csv)

    if validate:
        check_validity(df)

    transform(df)

    json_filename = os.path.splitext(csv.name)[0]
    json_filename += '.json'
    df.to_json(json_filename, orient='records', indent=2)    

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
