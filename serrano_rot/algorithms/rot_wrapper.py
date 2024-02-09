import sys
import json
import importlib
import traceback


if __name__ == "__main__":

    if len(sys.argv) != 4:
        sys.stderr.write("Wrong number of arguments")
        sys.exit(1)

    try:

        module_name = sys.argv[1]
        parameters = sys.argv[2]
        infrastructure = sys.argv[3]

        # Import required module for the algorithm execution
        algorithm_module = importlib.import_module("%s.%s" % (module_name, module_name))

        # Get algorithm main class, by convention its name should be the same as the algorithm module.
        algorithm_class = getattr(algorithm_module, module_name)
        algo_obj = algorithm_class(parameters, infrastructure)

        # Execute the selected algorithm with the provided input parameters
        results = algo_obj.launch()

        # Write the output
        sys.stdout.write(results)

    except ImportError as e:
        sys.stderr.write("Unable to import the required module: %s" % module_name)
        sys.exit(1)
    except Exception as e:
        sys.stderr.write("Execution Failed.")
        sys.stderr.write(traceback.format_exc())
        sys.exit(1)