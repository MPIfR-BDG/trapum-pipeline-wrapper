import os
import sys
import time
import json
import glob
import shlex
import shutil
import tarfile
import optparse
import subprocess
import logging
import pika_wrapper
import multiprocessing
from multiprocessing.pool import ThreadPool
import numpy as np
import pandas as pd
import xml.etree.ElementTree as ET
import parseheader
from trapum_pipeline_wrapper import TrapumPipelineWrapper


log = logging.getLogger('dspsr_folder')
FORMAT = "[%(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
logging.basicConfig(format=FORMAT)
log.setLevel('INFO')


def make_tarfile(output_path, input_path, name):
    with tarfile.open(output_path + '/' + name, "w:gz") as tar:
        tar.add(input_path, arcname=os.path.basename(input_path))


def a_to_pdot(P_s, acc_ms2):
    LIGHT_SPEED = 2.99792458e8                 # Speed of Light in SI
    return P_s * acc_ms2 / LIGHT_SPEED


def period_modified(p0, pdot, no_of_samples, tsamp, fft_size):
    """
    returns period with reference to the middle epoch of observation
    """
    if (fft_size == 0.0):
        return p0 - pdot * \
            float(1 << (no_of_samples.bit_length() - 1) - no_of_samples) * tsamp / 2
    else:
        return p0 - pdot * float(fft_size - no_of_samples) * tsamp / 2


def remove_dir(dir_name):
    if 'TEMP' in dir_name:
        shutil.rmtree(dir_name)
    else:
        log.error("Directory not deleted. Not a temporary folder!")


def untar_file(tar_file, tmp_dir):
    try:
        subprocess.check_call(
            "tar -zxvf %s -C %s" %
            (tar_file, tmp_dir), shell=True)
    except Exception as error:
        log.error(error)
        raise error


def execute_command(command, output_dir):
    subprocess.check_call(command, shell=True, cwd=output_dir)


def generate_pulsarX_cand_file(
        tmp_dir,
        beam_name,
        utc_name,
        cand_mod_periods,
        cand_dms,
        cand_accs,
        cand_snrs):

    cand_file_path = '%s/%s_%s_cands.candfile' % (tmp_dir, beam_name, utc_name)
    source_name_prefix = "%s_%s" % (beam_name, utc_name)
    with open(cand_file_path, 'w') as f:
        f.write("#id DM accel F0 F1 S/N\n")
        for i in range(len(cand_mod_periods)):
            f.write(
                "%d %f %f %f 0 %f\n" %
                (i,
                 cand_dms[i],
                    cand_accs[i],
                    1.0 /
                    cand_mod_periods[i],
                    cand_snrs[i]))
        f.close()

    return cand_file_path


def parse_pdmp_stdout(stream):
    for line in stream.splitlines():
        if line.startswith("Best DM"):
            dm = float(line.split()[3])
            break
        else:
            raise Exception("no best DM")
    for line in stream.splitlines():
        if line.startswith("Best TC Period"):
            tc = float(line.split()[5])
            break
        else:
            raise Exception("no best TC period")
    return tc, dm


def convert_to_std_format(ra, dec):
    # Convert hour angle strings to degrees

    ra_deg = int(ra / 10000)
    ra_min = int(ra / 100) - 100 * ra_deg
    ra_sec = ra - 10000 * ra_deg - 100 * ra_min

    dec_deg = int(dec / 10000)
    dec_min = int(dec / 100) - 100 * dec_deg
    dec_sec = dec - 10000 * dec_deg - 100 * dec_min

    ra_coord = "{}:{}:{:.2f}".format(ra_deg, abs(ra_min), abs(ra_sec))
    # print(ra_coord)
    dec_coord = "{}:{}:{:.2f}".format(dec_deg, abs(dec_min), abs(dec_sec))

    return ra_coord, dec_coord


def get_obs_length(filterbanks):
    return sum([get_fil_dict(fname)['tobs'] for fname in filterbanks])


def get_fil_dict(input_file):
    filterbank_info = parseheader.parseSigprocHeader(input_file)
    filterbank_stats = parseheader.updateHeader(filterbank_info)
    return filterbank_stats


def parse_cuts(cuts, tobs):
    if ":" not in cuts:
        return float(cuts)
    else:
        for cut in cuts.split(","):
            low,high,value = list(map(float, cut.split(":")))
            if tobs >= low and tobs < high:
                return value
        else:
            return 0.0


def fold_and_score_pipeline(data):
    '''
    required from pipeline: Filetype, filename, beam id , pointing id, directory

    '''
    tstart = time.time()
    output_dps = []
    dp_list = []

    processing_args = data['processing_args']
    output_dir = data['base_output_dir']
    processing_id = data['processing_id']

    # Make output dir
    try:
        subprocess.check_call("mkdir -p %s" % (output_dir), shell=True)
    except BaseException:
        log.info("Already made subdirectory")
        pass

    # Make temporary folder to keep any temporary outputs
    tmp_dir = '/beeond/PROCESSING/TEMP/%d' % processing_id
    try:
        subprocess.check_call("mkdir -p %s" % (tmp_dir), shell=True)
    except BaseException:
        log.info("Already made subdirectory")
        pass

    # Get the beam info
    for pointing in data["data"]["pointings"]:
        utc_start = pointing['utc_start']
        for beam in pointing["beams"]:
            input_fil_list = []
            for dp in (beam["data_products"]):
                if '.fil' in dp["filename"]:
                    input_fil_list.append(dp["filename"])
                elif '.tar.gz' in dp['filename']:
                    tarred_csv = dp["filename"]
                beam_ID = int(beam["id"])
                beam_name = beam["name"]

            input_fil_list.sort()
            input_filenames = ' '.join(input_fil_list)

            # Untar csv file
            log.info(
                "Untarring the filtered candidate information to %s" %
                tmp_dir)
            untar_file(tarred_csv, tmp_dir)
            # tmp_dir = glob.glob(tmp_dir + '/' + os.path.basename(tarred_csv)
            tmp_dir = glob.glob(tmp_dir + '/**/')[0]

            # Read candidate info file into Pandas Dataframe
            log.info("Reading candidate info...")
            cand_file = glob.glob(
                '%s/*good_cands_to_fold_with_beam.csv' %
                (tmp_dir))[0]
            df = pd.read_csv(cand_file)

            # Select only candidates with corresponding beam id and snr cutoff
            log.info(
                "Selecting candidates for  beam ID %d and SNR higher than %f" %
                (beam_ID, processing_args['snr_cutoff']))
            snr_cut_cands = df[df['snr'] > float(
                processing_args['snr_cutoff'])]
            period_cuts = processing_args.get('period_cutoffs', "0:inf:0.0000000001")
            obs_length = get_obs_length(input_fil_list)
            log.info("Parsing period cuts: {}".format(period_cuts))
            period_cut = parse_cuts(period_cuts, obs_length)
            log.info("Selecting periods above {} seconds".format(period_cut))
            snr_cut_cands = snr_cut_cands[snr_cut_cands['period'] > period_cut]
            single_beam_cands = snr_cut_cands[snr_cut_cands['beam_id'] == beam_ID]
            single_beam_cands.sort_values('snr', inplace=True, ascending=False)
            log.info("Found {} candidates to fild".format(len(single_beam_cands)))
            print(single_beam_cands)

            # If no candidates found in this beam, skip to next message
            if single_beam_cands.shape[0] == 0:
                raise Exception("No candidate found to fold")

            # Limit number of candidates to fold
            log.info(
                "Setting a maximum limit of %d candidates per beam" %
                (processing_args['cand_limit_per_beam']))
            if single_beam_cands.shape[0] > processing_args['cand_limit_per_beam']:
                single_beam_cands_fold_limited = single_beam_cands.head(
                    processing_args['cand_limit_per_beam'])
            else:
                single_beam_cands_fold_limited = single_beam_cands

            # Read parameters and fold
            log.info("Reading all necessary candidate parameters")
            cand_periods = single_beam_cands_fold_limited['period'].to_numpy()
            cand_accs = single_beam_cands_fold_limited['acc'].to_numpy()
            cand_dms = single_beam_cands_fold_limited['dm'].to_numpy()
            cand_snrs = single_beam_cands_fold_limited['snr'].to_numpy()
            cand_ids = single_beam_cands_fold_limited['cand_id_in_file'].to_numpy(
            )
            # Choose first element. If filtered right, there should be just one
            # xml filename throughout!
            xml_files = single_beam_cands_fold_limited['file'].to_numpy()

            tree = ET.parse(xml_files[0])
            root = tree.getroot()
            tsamp = float(root.find("header_parameters/tsamp").text)
            fft_size = float(root.find('search_parameters/size').text)
            no_of_samples = int(root.find("header_parameters/nsamples").text)

            log.info("Modifying period to middle epoch reference of file")

            mod_periods = []
            pdots = []
            for i in range(len(cand_periods)):
                Pdot = a_to_pdot(cand_periods[i], cand_accs[i])
                mod_periods.append(
                    period_modified(
                        cand_periods[i],
                        Pdot,
                        no_of_samples,
                        tsamp,
                        fft_size))
                pdots.append(Pdot)

            cand_mod_periods = np.asarray(mod_periods, dtype=float)

            log.info("Generating predictor file for PulsarX")
            try:
                pred_file = generate_pulsarX_cand_file(
                    tmp_dir,
                    beam_name,
                    utc_start,
                    cand_mod_periods,
                    cand_dms,
                    cand_accs,
                    cand_snrs)
                log.info("Predictor file ready for folding: %s" % (pred_file))
            except Exception as error:
                log.error(error)
                log.error("Predictor candidate file generation failed")

            # Run PulsarX
            log.info("PulsarX will be launched with the following command:")

            cmask = processing_args.get("channel_mask", None)
            zap_string = ""
            if cmask is not None:
                cmask = cmask.strip()
                try:
                    zap_string = " ".join(["--rfi zap {} {}".format(
                        *i.split(":")) for i in cmask.split(",")])
                except Exception as error:
                    raise Exception("Unable to parse channel mask: {}".format(
                        str(error)))

            nbins = processing_args.get("nbins", 32)
            subint_length = processing_args.get("subint_length", 10.0)
            nsubband = processing_args.get("nsubband", 64)
            try:
                if 'ifbf' in beam_name:  # Decide beam name in output
                    script = "psrfold_fil -v -t 12 --candfile %s -n %d -b %d --incoherent --template /home/psr/software/PulsarX/include/template/meerkat_fold.template --clfd 2.0 -L %d -f %s %s" % (
                        pred_file, nsubband, nbins, subint_length, input_filenames, zap_string)
                    log.info(script)
                    subprocess.check_call(script, shell=True, cwd=tmp_dir)
                    log.info("PulsarX folding successful")

                elif 'cfbf' in beam_name:
                    beam_no = int(beam_name.strip("cfbf"))
                    script = "psrfold_fil -v -t 12 --candfile %s -n %d -b %d -i %d --template /home/psr/software/PulsarX/include/template/meerkat_fold.template -L %d --clfd 2.0 -f %s %s" % (
                        pred_file, nsubband, nbins, beam_no, subint_length, input_filenames, zap_string)
                    log.info(script)
                    subprocess.check_call(script, shell=True, cwd=tmp_dir)
                    log.info("PulsarX folding successful")

                else:
                    log.info("Invalid beam name. Folding with default beam name")
                    script = "psrfold_fil -v -t 12 --candfile %s -n %d -b %d  --template /home/psr/software/PulsarX/include/template/meerkat_fold.template -L %d --clfd 2.0 -f %s %s" % (
                        pred_file, nsubband, nbins, subint_length, input_filenames, zap_string)
                    log.info(script)
                    subprocess.check_call(script, shell=True, cwd=tmp_dir)
                    log.info("PulsarX folding successful")

            except Exception as error:
                log.error(error)
                log.error("PulsarX failed")

            log.info("Folding done for all candidates. Scoring all candidates...")
            subprocess.check_call(
                "python2 webpage_score.py --in_path={}".format(tmp_dir),
                shell=True)
            log.info("Scoring done...")

            subprocess.check_call(
                "rm *.csv",
                shell=True,
                cwd=tmp_dir)  # Remove the input csv files

            # Copy over the relevant meta file
            meta_file_path = input_fil_list[0].split(beam_name)[0]
            subprocess.check_call(
                "cp %s/apsuse.meta %s.meta" %
                (meta_file_path, utc_start), shell=True, cwd=tmp_dir)

            # Decide tar name
            tar_name = os.path.basename(
                output_dir) + "_folds_and_scores.tar.gz"

            # Generate new metadata csv file
            df1 = pd.read_csv(
                glob.glob(
                    "{}/*.cands".format(tmp_dir))[0],
                skiprows=11,
                delim_whitespace=True)
            df2 = pd.read_csv("{}/pics_scores.txt".format(tmp_dir))
            df1['png_file'] = [
                output_dir +
                "/" +
                tar_name +
                "/" +
                os.path.basename(
                    ar.replace(
                        ".ar",
                        ".png")) for ar in df2['arfile']]
            df1['ar_file'] = [output_dir + "/" + tar_name + "/" +
                              os.path.basename(ar) for ar in df2['arfile']]
            df1['pics_TRAPUM_Ter5'] = df2['clfl2_trapum_Ter5.pkl']
            df1['pics_PALFA'] = df2['clfl2_PALFA.pkl']

            with open(glob.glob("{}/*.cands".format(tmp_dir))[0], "r") as f:
                comment_lines = []
                for ln in f:
                    if ln.startswith("#"):
                        comment_lines.append(ln)
                comment_lines = comment_lines[:-1]
                f.close()

            with open("{}/{}_{}_metadata.csv".format(tmp_dir, beam_name, utc_start), "w") as f:
                for line in comment_lines:
                    f.write(line)
                df1.to_csv(f)
                f.close()

            # Remove txt files
            subprocess.check_call("rm *.txt", shell=True, cwd=tmp_dir)

            # Create tar file of tmp directory in output directory
            log.info("Tarring up all folds and the metadata csv file")
            #tar_name = os.path.basename(output_dir) + "_folds_and_scores.tar.gz"
            make_tarfile(output_dir, tmp_dir, tar_name)
            log.info("Tarred")

            # Remove contents in temporary directory
            remove_dir(tmp_dir)
            log.info("Removed temporary files")

            # Add tar file to dataproduct
            dp = dict(
                type="fold_tar_file_pulsarx_20201214",
                filename=tar_name,
                directory=output_dir,
                beam_id=beam_ID,
                pointing_id=pointing["id"],
                metainfo=json.dumps("tar_file:folded_archives")
            )

            output_dps.append(dp)

    tend = time.time()
    print ("Time taken is : %f s" % (tend - tstart))
    return output_dps


if __name__ == "__main__":

    parser = optparse.OptionParser()
    pika_wrapper.add_pika_process_opts(parser)
    TrapumPipelineWrapper.add_options(parser)
    opts, args = parser.parse_args()

    processor = pika_wrapper.pika_process_from_opts(opts)
    pipeline_wrapper = TrapumPipelineWrapper(opts, fold_and_score_pipeline)
    processor.process(pipeline_wrapper.on_receive)

    get_params_from_csv_and_fold(opts)

