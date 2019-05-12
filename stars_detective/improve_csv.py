import pandas as pd
import sys



from utils import try_toget_format_wrap


if __name__ == '__main__':
    n_cores = int(sys.argv[1])

    resources_df = pd.read_csv("input_files/resources.csv", sep=";")


    try_toget_format_wrap(resources_df, n_cores=n_cores)
