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
import mongo_wrapper
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

TEMPLATE = "/home/psr/software/PulsarX/include/template/meerkat_fold.template"


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
            float(1 << (no_of_samples.bit_length() - 1) -
                  no_of_samples) * tsamp / 2
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
        cand_snrs,
        batch_start,
        batch_stop):

    cand_file_path = '%s/%s_%s_%d_%d_cands.candfile' % (
        tmp_dir, beam_name, utc_name, batch_start, batch_stop-1)
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
            low, high, value = list(map(float, cut.split(":")))
            if tobs >= low and tobs < high:
                return value
        else:
            return 0.0


def reoptimize_and_score_pipeline(data, status_callback):
    '''
    required from pipeline: Filetype, filename, beam id , pointing id, directory

    '''
    tstart = time.time()
    output_dps = []
    dp_list = []

    processing_args = data['processing_args']
    output_dir = data['base_output_dir']
    processing_id = data['processing_id']

    os.environ["OMP_NUM_THREADS"] = "1"

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
            for dp in (beam["data_products"]):
                if '.tar.gz' in dp['filename']:
                    tarred_csv = dp["filename"]
                beam_ID = int(beam["id"])
                beam_name = beam["name"]

            # Untar csv file
            log.info(
                "Untarring the folds_and_scores to %s" %
                tmp_dir)
            untar_file(tarred_csv, tmp_dir)
            # tmp_dir = glob.glob(tmp_dir + '/' + os.path.basename(tarred_csv)

            status_callback("reoptimize archives")
            log.info("check candfile and archives...")
            cand_file = glob.glob(
                '%s/*.cands' %
                (tmp_dir))[0]
            arch_file = glob.glob(
                '%s/*.ar' %
                (tmp_dir))
            arch_file.sort()

            scale = processing_args.get("scale", 1.)
            cmask = processing_args.get("channel_mask", None)
            zap_string = ""
            if cmask is not None:
                cmask = cmask.strip()
                if cmask:
                    try:
                        zap_string = " ".join(["--zap {} {}".format(
                            *i.split(":")) for i in cmask.split(",")])
                    except Exception as error:
                        raise Exception("Unable to parse channel mask: {}".format(
                            str(error)))

            script = f"dmffdot --scale {scale} --correct --update --candfile {cand_file} --template {TEMPLATE} --plotx {zap_string} -f {' '.join(arch_file)}"
            log.info(script)
            try:
                subprocess.check_call(script, shell=True, cwd=tmp_dir)
            except Exception as error:
                raise error
            log.info("PulsarX reoptimize successful")

            status_callback("Scoring candidates")
            log.info("Folding done for all candidates. Scoring all candidates...")
            subprocess.check_call(
                "python2 webpage_score.py --in_path={}".format(tmp_dir),
                shell=True)
            log.info("Scoring done...")

            subprocess.check_call(
                "rm *.csv",
                shell=True,
                cwd=tmp_dir)  # Remove the input csv files

            # Decide tar name
            tar_name = os.path.basename(
                output_dir) + "_reoptimize_and_scores.tar.gz"

            # Generate new metadata csv file
            cand_files = sorted(glob.glob("{}/*.cands".format(tmp_dir)))
            dfs = []
            for cf in cand_files:
                dfs.append(pd.read_csv(cf,
                                       skiprows=11,
                                       delim_whitespace=True))
            df1 = pd.concat(dfs, axis=0, ignore_index=True)
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
            try:
                remove_dir(tmp_dir)
                log.info("Removed temporary files")
            except Exception as error:
                log.exception("Unable to remove temp dir")

            # Add tar file to dataproduct
            dp = dict(
                type="batch_reoptimize_tar_file_pulsarx",
                filename=tar_name,
                directory=output_dir,
                beam_id=beam_ID,
                pointing_id=pointing["id"],
                metainfo=json.dumps({"scale": scale, "zap_string": zap_string})
            )

            output_dps.append(dp)

    tend = time.time()
    print("Time taken is : %f s" % (tend - tstart))
    return output_dps


if __name__ == "__main__":
    parser = optparse.OptionParser()
    mongo_wrapper.add_mongo_consumer_opts(parser)
    TrapumPipelineWrapper.add_options(parser)
    opts, args = parser.parse_args()

    # processor = mongo_wrapper.PikaProcess(...)
    processor = mongo_wrapper.mongo_consumer_from_opts(opts)
    pipeline_wrapper = TrapumPipelineWrapper(
        opts, reoptimize_and_score_pipeline)
    processor.process(pipeline_wrapper.on_receive)
