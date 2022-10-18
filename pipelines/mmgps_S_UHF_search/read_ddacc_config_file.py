''' Generate a DDACC Plan to be passed onto Peasoup based on a configuration file. 
    Requires Presto installation to run DDPlan.py '''

import pandas as pd 
import configparser, subprocess, argparse

parser = argparse.ArgumentParser(description='Generate a DDACC Plan to be passed onto Peasoup based on a configuration file')
parser.add_argument('-c', '--config_file', help='Configuration file based on metadata and search range', type=str,  default="sample_ddacc.cfg")


def main():
    args = parser.parse_args()
    config_file = args.config_file
    config = configparser.ConfigParser()
    config.read(config_file)
    sections = config.sections()
    central_freq = float(config['OBSERVATION_METADATA']['CENTRAL_FREQUENCY(MHz)'])
    bandwidth = int(config['OBSERVATION_METADATA']['BANDWIDTH(MHz)'])
    nchans = int(config['OBSERVATION_METADATA']['NCHANS'])
    presto_path = config['PRESTO_PATH']['PRESTO']

    ddacc_plan = {}
    for search_type in sections:
        
        if not (search_type == "PRESTO_PATH" or search_type == "OBSERVATION_METADATA"):
            ddacc_plan[search_type] = {}
            
            
            tsamp = float(config[search_type]['TSAMP_ORIG(microseconds)'])/1e6
            dd_min = float(config[search_type]['DM_MIN'])
            dd_max = float(config[search_type]['DM_MAX'])
            
            ddplan_command = presto_path + "bin/" + "DDplan.py -o MMGPS_ddplan -l %.2f -d %.2f -f %.2f -b %d -n %d -t %.6f" \
            %(dd_min, dd_max, central_freq, bandwidth, nchans, tsamp)
            ddplan_command = ddplan_command.split(' ')
            with open("DDPlan_results_%s.txt" %search_type,"w") as f:
                ddplan_output = subprocess.run(ddplan_command, check=True, stdout=f)
            df = pd.read_csv('DDPlan_results_%s.txt' %search_type, delim_whitespace=True, skiprows=11,
                            names=['Low_DM', 'High_DM', 'dDM', 'DownSamp', 'Ndm', 'WorkFract'])
            
            del df['WorkFract']
            ddplan_results = df.to_numpy()
            start_dm, end_dm, ddM, DownSamp,  Ndm = ddplan_results[:,0], ddplan_results[:,1], ddplan_results[:,2], ddplan_results[:,3], ddplan_results[:,4]
            segment_list = config[search_type]['LIST_SEGMENTS'].split(',')
            acceleration_min_list = config[search_type]['ACC_MIN(ms-2)'].split(',')
            acceleration_max_list = config[search_type]['ACC_MAX(ms-2)'].split(',')
            nharmonic_sums = config[search_type]['NHARMONIC_SUMS'].split(',')

            #Assign last value of acceleration provided by user for segemented searchess
            if len(acceleration_min_list) != len(segment_list):
                difference = int(len(segment_list) - len(acceleration_min_list))
                acceleration_min_list += difference * [acceleration_min_list[-1]]

            if len(acceleration_max_list) != len(segment_list):
                difference = int(len(segment_list) - len(acceleration_max_list))
                acceleration_max_list += difference * [acceleration_max_list[-1]]

            if len(nharmonic_sums) != len(segment_list):
                difference = int(len(segment_list) - len(nharmonic_sums))
                nharmonic_sums += difference * [nharmonic_sums[-1]]
            

            for i in range(len(segment_list)):
                ddacc_plan[search_type][segment_list[i]] = []
                for j in range(len(start_dm)):
                    ddacc_plan[search_type][segment_list[i]].append([start_dm[j], end_dm[j], ddM[j], \
                    DownSamp[j], Ndm[j], float(acceleration_min_list[i]), float(acceleration_max_list[i]),int(nharmonic_sums[i])])

    print(ddacc_plan)

if __name__ == "__main__":
    main()




        
        
       
        

        




