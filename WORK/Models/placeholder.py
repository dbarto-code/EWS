
import pandas as pd
import os

#%% 
def load_directory(local_dir, remote_dir):
    return local_dir if os.path.exists(local_dir) else remote_dir

local_directory = 'C:\\Users\\Daniel.Barto\\Documents\\GitHub\EWS\\'
remote_directory = 'OTHER'

main_dir = load_directory(local_directory, remote_directory)


