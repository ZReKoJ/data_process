



from lib.data_process_lib import * 

def parse_arguments():
    # Create argument parser
    parser = argparse.ArgumentParser()

    # Positional mandatory arguments
    parser.add_argument("file", help="Flow Configuration file", type=str)

    # Optional arguments with parameter
    parser.add_argument("-i", "--id", type=str, help="Execution ID", action="append", default=[get_time(dateformat="%Y%m%d%H%M%S")])
    
    # Optional arguments without parameter
    parser.add_argument("--show-command", help="Show the commands to execute the components", action="store_true")

    # Others
    parser.add_argument("--version", help="Check Version", action="version", version='%(prog)s - Version 1.0')

    # Parse arguments
    args = vars(parser.parse_args())
    args["id"] = "_".join(args.get("id"))

    return args

if __name__ == '__main__': 

    args = parse_arguments()

    print(args)





