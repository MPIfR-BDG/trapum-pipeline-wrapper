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

log = logging.getLogger("transientx_search")
FORMAT = "[%(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
logging.basicConfig(format=FORMAT)
log.setLevel("INFO")


def make_tarfile(output_path, input_path, name):
    with tarfile.open(output_path + '/' + name, "w:gz") as tar:
        tar.add(input_path, arcname=os.path.basename(input_path))


DMRange = namedtuple(
    "DMRange",
    ["low_dm", "high_dm", "dm_step", "tscrunch"])


class DDPlan(object):
    def __init__(self):
        self._segments = []

    def add_range(self, low_dm, high_dm, step, tscrunch):
        self._segments.append(DMRange(low_dm, high_dm, step, tscrunch))

    def __iter__(self):
        return iter(sorted(self._segments, key=lambda x: x.tscrunch))

    def __str__(self):
        out = []
        for r in self._segments:
            out.append(str(r))
        return "\n".join(out)

    @classmethod
    def from_string(cls, plan):
        inst = cls()
        for line in plan.splitlines():
            low_dm, high_dm, dm_step, tscrunch = list(
                map(float, line.split()[:4]))
            inst.add_range(low_dm, high_dm, dm_step, int(tscrunch))
        return inst


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
            log.warning(
                f"Removing existing file with name {os.path.join(dir, file)}")
            os.remove(os.path.join(dir, file))


async def transientx(input_fils, output_dir,
                     ddplan_args,
                     fscrunch,
                     snr_threshold,
                     max_search_width,
                     rfi_flags,
                     beam_tag,
                     rootname="J0000-00",
                     num_threads=12,
                     zapping_threshold=3.0,
                     snrloss=0.1,
                     segment_length=1.0,
                     overlap=0.1,
                     dbscan_radius=1.,
                     dbscan_k=2,
                     minimum_points_limit=5,
                     baseline_pre=0.0,
                     baseline_post=0.1,
                     drop_cands_with_maxwidth=False):
    delete_files_if_exists(output_dir)

    # generate ddplan file
    try:
        log.info("Parsing DDPlan")
        ddplan = DDPlan.from_string(ddplan_args)
    except Exception as error:
        log.exception("Unable to parse DDPlan")
        raise error

    ddplan_fname = os.path.join(output_dir, "ddplan.txt")

    with open(ddplan_fname, 'w') as ddplan_file:
        for dm_range in ddplan:
            segment = f"{dm_range.tscrunch} 1 {dm_range.low_dm} {dm_range.dm_step} {np.int(np.ceil((dm_range.high_dm-dm_range.low_dm)/dm_range.dm_step))} {snrloss} {max_search_width} \n"
            ddplan_file.write(segment)

    drop_flag = ""
    if drop_cands_with_maxwidth:
        drop_flag = "--drop"
    cmd = f"transientx_fil -v -o {rootname} -t {num_threads} --zapthre {zapping_threshold} --fd {fscrunch} --overlap {overlap} --ddplan {ddplan_fname} --thre {snr_threshold} --maxw {max_search_width} --snrloss {snrloss} -l {segment_length} -r {dbscan_radius} -k {dbscan_k} --minpts {minimum_points_limit} --baseline {baseline_pre} {baseline_post} {drop_flag} {beam_tag} --fillPatch rand -z {rfi_flags} -f {' '.join(input_fils)}"

    # run transientx
    try:
        await shell_call(cmd, cwd=output_dir)
    except Exception as error:
        log.error("transientx_fil call failed, cleaning up partial files")
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


async def transientx_pipeline(data, status_callback):
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
            beam_name = beam["name"]
            dps = sorted(select_data_products(
                beam, lambda fname: fname.endswith(".fil")))

            if processing_args["temp_filesystem"] == "/beeond/":
                log.info("Running on Beeond")
                processing_dir = os.path.join(
                    BEEOND_TEMP_DIR, str(processing_id))
            else:
                log.info("Running on BeeGFS")
                processing_dir = os.path.join(output_dir, "processing/")
            os.makedirs(processing_dir, exist_ok=True)
            try:
                log.info("Executing transient search")
                fscrunch = processing_args.get("fscrunch", 1)
                snr_threshold = processing_args.get("snr_threshold", 7)
                max_search_width = processing_args.get(
                    "max_search_width", 0.05)
                rfi_flags = processing_args.get("rfi_flags", 1)
                ddplan_args = processing_args["ddplan"]

                cmask = processing_args.get("channel_mask", None)
                if cmask is not None:
                    cmask = cmask.strip()
                    if cmask:
                        try:
                            rfi_flags += " " + " ".join(["zap {} {}".format(
                                *i.split(":")) for i in cmask.split(",")])
                        except Exception as error:
                            raise Exception("Unable to parse channel mask: {}".format(
                                str(error)))

                if 'ifbf' in beam_name:
                    beam_tag = "--incoherent"
                elif 'cfbf' in beam_name:
                    beam_tag = "-i {}".format(int(beam_name.strip("cfbf")))
                else:
                    log.warning(
                        "Invalid beam name. Folding with default beam name")
                    beam_tag = ""
                status_callback("Transient search")
                await transientx(dps, processing_dir,
                                 ddplan_args,
                                 fscrunch,
                                 snr_threshold,
                                 max_search_width,
                                 rfi_flags,
                                 beam_tag,
                                 "%d_%d" % (pointing["id"], beam_ID))

                # count number of candidates
                ncands = len(glob.glob(processing_dir+"/*.png"))

                # Decide tar name
                tar_name = "%d_%d_transientx_candidates.tar.gz" % (
                    pointing["id"], beam_ID)

                # Create tar file of tmp directory in output directory
                log.info("Tarring up all transientx output files")
                make_tarfile(output_dir, processing_dir, tar_name)
                log.info("Tarred")

                # Add tar file to dataproduct
                dp = dict(
                    type="batch_tar_file_transientx",
                    filename=tar_name,
                    directory=output_dir,
                    beam_id=beam_ID,
                    pointing_id=pointing["id"],
                    metainfo=json.dumps({"number_of_candidates": ncands, "fscrunch": fscrunch,
                                        "snr_threshold": snr_threshold, "rfi_flags": rfi_flags, "ddplan_args": ddplan_args})
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
        transientx_pipeline(data, status_callback))
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
