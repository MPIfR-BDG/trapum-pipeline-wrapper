import optparse
import logging
import os
import glob
import tarfile
import shutil
import asyncio
import json
import numpy as np
from collections import namedtuple
import mongo_wrapper
from trapum_pipeline_wrapper import TrapumPipelineWrapper

BEEOND_TEMP_DIR = "/beeond/PROCESSING/TEMP/"

log = logging.getLogger("transientx_replot")
FORMAT = "[%(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
logging.basicConfig(format=FORMAT)
log.setLevel("INFO")


def make_tarfile(output_path, input_path, name):
    with tarfile.open(output_path + '/' + name, "w:gz") as tar:
        tar.add(input_path, arcname=os.path.basename(input_path))

async def shell_call(cmd, cwd="./"):
    log.info(f"Shell call: {cmd}")
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=None,
        stderr=None,
        cwd=cwd)
    retcode = await proc.wait()
    if retcode != 0:
        raise Exception(f"Process return-code {retcode}")

def delete_files_if_exists(dir):
    files = os.listdir(dir)
    for file in files:
        if file.endswith(".cands") or file.endswith(".png") or file.endswith(".txt"):
            log.warning(f"Removing existing file with name {os.path.join(dir, file)}")
            os.remove(os.path.join(dir, file))


async def transientx_replot(input_fils, input_txdps, output_dir,
                     tscrunch,
                     fscrunch,
                     dm_cutoff,
                     width_cutoff,
                     snr_cutoff,
                     zap_flags,
                     nwidth = 20,
                     num_threads = 12,                     
                     zapping_threshold = 3.0,
                     snrloss = 0.1):
    delete_files_if_exists(output_dir)

    txtar = tarfile.open(input_txdps)
    for file in txtar.getmembers():
        if file.name.endswith('.cands'):
            candfile = file
    txtar.extractall(output_dir, members=[candfile])

    cmd = f"replot_fil -v -t {num_threads} --zapthre {zapping_threshold} --td {tscrunch} --fd {fscrunch} --zdot --kadane 4 4 7 --clip 4 4 7 --dmcutoff {dm_cutoff} --widthcutoff {width_cutoff} --snrcutoff {snr_cutoff} --snrloss {snrloss} --zap {zap_flags} --candfile {candfile.name} --clean -f {' '.join(input_fils)}"

    # run transientx replot
    try:
        await shell_call(cmd, cwd=output_dir)
    except Exception as error:
        log.error("replot_fil call failed, cleaning up partial files")
        delete_files_if_exists(output_dir)
        raise error


def select_data_products(beam, filter_func=None):
    dp_list = []
    for dp in (beam["data_products"]):
        if filter_func:
            if filter_func(dp["filename"]):
                dp_list.append(dp["filename"])
        else:
            dp_list.append(dp["filename"])
    return dp_list


async def transientx_replot_pipeline(data, status_callback):
    # Limit core size to avoid ephemeral-storage overflows
    # in the event of segmentation faults
    await shell_call("ulimit -c  0")

    processing_args = data["processing_args"]
    output_dir = data["base_output_dir"]
    processing_id = data["processing_id"]

    log.info(f"Creating output directory: {output_dir}")
    # Make output dir
    os.makedirs(output_dir, exist_ok=True)
    output_dps = []

    for pointing in data["data"]["pointings"]:
        for beam in pointing["beams"]:

            beam_ID = int(beam["id"])
            dps = sorted(select_data_products(
                beam, lambda fname: fname.endswith(".fil")))

            dps_tx = sorted(select_data_products(
                beam, lambda fname: fname.endswith(".tar.gz")))

            if processing_args["temp_filesystem"] == "/beeond/":
                log.info("Running on Beeond")
                processing_dir = os.path.join(BEEOND_TEMP_DIR, str(processing_id))
            else:
                log.info("Running on BeeGFS")
                processing_dir = os.path.join(output_dir, "processing/")
            os.makedirs(processing_dir, exist_ok=True)
            try:
                log.info("Executing transient replot")
                tscrunch = processing_args.get("tscrunch", 1)
                fscrunch = processing_args.get("fscrunch", 1)
                dm_cutoff = processing_args.get("dm_cutoff", 1)
                width_cutoff = processing_args.get("width_cutoff", 0.1)
                snr_cutoff = processing_args.get("snr_cutoff", 7)

                cmask = processing_args.get("channel_mask", None)
                if cmask is not None:
                    cmask = cmask.strip()
                    if cmask:
                        try:
                            zap_flags = " " + " ".join(["{} {}".format(
                                *i.split(":")) for i in cmask.split(",")])
                        except Exception as error:
                            raise Exception("Unable to parse channel mask: {}".format(
                                str(error)))

                status_callback("Transient replot")
                await transientx_replot(dps, dps_tx[-1], processing_dir,
                                 tscrunch,
                                 fscrunch,
                                 dm_cutoff,
                                 width_cutoff,
                                 snr_cutoff,
                                 zap_flags)

                # count number of candidates
                ncands = len(glob.glob(processing_dir+"/*.png"))

                # Decide tar name
                tar_name = "%d_%d_transientx_candidates_replot.tar.gz"  %(pointing["id"], beam_ID)

                # Create tar file of tmp directory in output directory
                log.info("Tarring up all transientx output files")
                make_tarfile(output_dir, processing_dir, tar_name)
                log.info("Tarred")

                # Add tar file to dataproduct
                dp = dict(
                    type="batch_tar_file_transientx_replot",
                    filename=tar_name,
                    directory=output_dir,
                    beam_id=beam_ID,
                    pointing_id=pointing["id"],
                    metainfo=json.dumps({"number_of_candidates":ncands, "tscrunch":tscrunch, "fscrunch":fscrunch, "dm_cutoff":dm_cutoff, "width_cutoff":width_cutoff, "snr_cutoff":snr_cutoff, "zap_flags":zap_flags})
                )

                output_dps.append(dp)

            except Exception as error:
                raise error
            finally:
                shutil.rmtree(processing_dir)
    return output_dps


def pipeline(data, status_callback):
    loop = asyncio.get_event_loop()
    output_dps = loop.run_until_complete(
        transientx_replot_pipeline(data, status_callback))
    return output_dps


if __name__ == '__main__':
    parser = optparse.OptionParser()
    mongo_wrapper.add_mongo_consumer_opts(parser)
    TrapumPipelineWrapper.add_options(parser)
    opts, args = parser.parse_args()

    # processor = mongo_wrapper.PikaProcess(...)
    processor = mongo_wrapper.mongo_consumer_from_opts(opts)
    pipeline_wrapper = TrapumPipelineWrapper(opts, pipeline)
    processor.process(pipeline_wrapper.on_receive)
