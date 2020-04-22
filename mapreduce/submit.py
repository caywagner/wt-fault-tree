"""
MapReduce job submission script.

Before using this script, start the MapReduce server.
$ ./bin/mapreduce start

Then, submit a job.  Everything has a default.
$ mapreduce-submit

You can change any of the options.
$ mapreduce-submit --help
"""

import socket
import json
import click

# Configure hard coded options
HOST = "localhost"


# Configure command line options
@click.command()
@click.option(
    "--port", "-p", "port", default=6000,
    help="Master port number, default = 6000",
)
@click.option(
    "--input", "-i", "input_directory", default="tests/input",
    help="Input directory, default=tests/input",
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
)
@click.option(
    "--output", "-o", "output_directory", default="output",
    help="Output directory, default=output",
    type=click.Path(exists=False, file_okay=False, dir_okay=True),
)
@click.option(
    "--mapper", "-m", "mapper_executable",
    default="tests/exec/wc_map.sh",
    help="Mapper executable, default=tests/exec/wc_map.sh",
    type=click.Path(exists=True, file_okay=True, dir_okay=False),
)
@click.option(
    "--reducer", "-r", "reducer_executable",
    default="tests/exec/wc_reduce.sh",
    help="Reducer executable, default=tests/exec/wc_reduce.sh",
    type=click.Path(exists=True, file_okay=True, dir_okay=False),
)
@click.option(
    "--nmappers", "num_mappers", default=4, type=int,
    help="Number of mappers, default=4",
)
@click.option(
    "--nreducers", "num_reducers", default=1, type=int,
    help="Number of reducers, default=1",
)
def main(port,
         input_directory,
         output_directory,
         mapper_executable,
         reducer_executable,
         num_mappers,
         num_reducers):
    """Top level command line interface."""
    # We want a bunch of arguments, this is the top level CLI.
    # pylint: disable=too-many-arguments
    job_dict = {
        "message_type": "new_master_job",
        "input_directory": input_directory,
        "output_directory": output_directory,
        "mapper_executable": mapper_executable,
        "reducer_executable": reducer_executable,
        "num_mappers": num_mappers,
        "num_reducers": num_reducers
    }

    # Send the data to the port that master is on
    message = json.dumps(job_dict)
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((HOST, port))
        sock.sendall(str.encode(message))
        sock.close()
    except socket.error as err:
        print("Failed to send job to master.")
        print(err)

    # Print to CLI
    print("Submitted job to master {}:{}".format(HOST, port))
    print("input directory     ", input_directory)
    print("output directory    ", output_directory)
    print("mapper executable   ", mapper_executable)
    print("reducer executable  ", reducer_executable)
    print("num mappers         ", num_mappers)
    print("num reducers        ", num_reducers)


if __name__ == "__main__":
    # Click will provide the arguments, disable this pylint check.
    # pylint: disable=no-value-for-parameter
    main()
