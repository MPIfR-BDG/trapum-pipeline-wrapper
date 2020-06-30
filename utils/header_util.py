import ctypes as C
import os
import numpy as np
import struct
from struct import unpack
from os.path import splitext

# dictionary to define the sizes of header elements
header_keys = {
    "HEADER_START":None,
    "HEADER_END":None,
    "filename": 'str',
    "telescope_id": 'I',
    "telescope": 'str',
    "machine_id": 'I',
    "data_type": 'I',
    "rawdatafile": 'str',
    "source_name": 'str',
    "barycentric": 'I',
    "pulsarcentric": 'I',
    "az_start": 'd',
    "za_start": 'd',
    "src_raj": 'd',
    "src_dej": 'd',
    "tstart": 'd',
    "tsamp": 'd',
    "nbits": 'I',
    "nsamples": 'I',
    "fch1": 'd',
    "foff": 'd',
    "fchannel": 'd',
    "nchans": 'I',
    "nifs": 'I',
    "refdm": 'd',
    "flux": 'd',
    "period": 'd',
    "nbeams": 'I',
    "ibeam": 'I',
    "hdrlen": 'I',
    "pb":"d",
    "ecc":"d",
    "asini":"d",
    "orig_hdrlen": 'I',
    "new_hdrlen": 'I',
    "sampsize": 'I',
    "bandwidth": 'd',
    "fbottom": 'd',
    "ftop": 'd',
    "obs_date": 'str',
    "obs_time": 'str',
    "signed": 'b',
    "accel": 'd'}

# header keys recognised by the sigproc package
sigproc_keys = ["signed","telescope_id","ibeam","nbeams",
                "refdm","nifs","nchans","foff","fch1","nbits",
                "tsamp","tstart","src_dej","src_raj","za_start",
                "az_start","source_name","rawdatafile","data_type",
                "machine_id"]

# header units for fancy printing
header_units = {
    "az_start": "(Degrees)",
    "za_start": "(Degrees)",
    "src_raj": "(HH:MM:SS.ss)",
    "src_dej": "(DD:MM:SS.ss)",
    "tstart": '(MJD)',
    "tsamp": '(s)',
    "fch1": "(MHz)",
    "foff": "(MHz)",
    "fchannel": "(MHz)",
    "refdm": "(pccm^-3)",
    "period": "(s)",
    "pb":"(hrs)",
    "flux": "(mJy)",
    "hdrlen": "(Bytes)",
    "orig_hdrlen": "(Bytes)",
    "new_hdrlen": "(Bytes)",
    "sampsize": "(Bytes)",
    "bandwidth": "(MHz)",
    "fbottom": "(MHz)",
    "ftop": "(MHz)",
    "ra_rad":'(Radians)',
    "dec_rad":'(Radians)',
    "ra_deg":'(Degrees)',
    "dec_deg":'(Degrees)',
    "Glon":'(Degrees)',
    "Glat":'(Degrees)',
    "obs_date":'(dd/mm/yy)',
    "obs_time":'(hh:mm:ss.sssss)'
}

# data type flag for sigproc files
data_types = {
    1:"Filterbank file",
    2:"Timeseries file"}

# convert between types from the struct module and numpy
struct_to_numpy = {
    "I":"uint",
    "d":"float",
    "str":"S256"}

telescope_ids = {
    "Fake": 0,
    "Arecibo": 1,
    "Ooty": 2,
    "Nancay": 3,
    "Parkes": 4,
    "Jodrell": 5,
    "GBT": 6,
    "GMRT": 7,
    "Effelsberg": 8,
    "Effelsberg LOFAR":9,
    "Unknown": 10}

ids_to_telescope = dict(zip(telescope_ids.values(), telescope_ids.keys()))

machine_ids = {
    "FAKE": 0,
    "PSPM": 1,
    "Wapp": 2,
    "AOFTM": 3,
    "BCPM1": 4,
    "OOTY": 5,
    "SCAMP": 6,
    "GBT Pulsar Spigot": 7,
    "PFFTS": 8,
    "Unknown":9}

ids_to_machine = dict(zip(machine_ids.values(), machine_ids.keys()))

# not required (may be of use in future)
telescope_lats_longs = {
    "Effelsberg":(50.52485,6.883593)
    }

# convert between numpy dtypes and ctypes types.
nptypes_to_ctypes = {"|b1":C.c_bool,
                     "|S1":C.c_char,
                     "<i1":C.c_byte,
                     "<u1":C.c_ubyte,
                     "<i2":C.c_short,
                     "<u2":C.c_ushort,
                     "<i4":C.c_int,
                     "<u4":C.c_uint,
                     "<i8":C.c_long,
                     "<u8":C.c_ulong,
                     "<f4":C.c_float,
                     "<f8":C.c_double}

ctypes_to_nptypes = dict(zip(nptypes_to_ctypes.values(), nptypes_to_ctypes.keys()))

nbits_to_ctypes = {1:C.c_ubyte,
                   2:C.c_ubyte,
                   4:C.c_ubyte,
                   8:C.c_ubyte,
                   16:C.c_short,
                   32:C.c_float}

ctypes_to_nbits = dict(zip(nbits_to_ctypes.values(), nbits_to_ctypes.keys()))

nbits_to_dtype = {1:"<u1",
                  2:"<u1",
                  4:"<u1",
                  8:"<u1",
                  16:"<u2",
                  32:"<f4"}

# useful for creating inf files
inf_to_header = {
    'Telescope used':["telescope_id",str],
    'Instrument used':["machine_id",str],
    'Object being observed':["source_name",str],
    'J2000 Right Ascension (hh:mm:ss.ssss)':['src_raj',str],
    'J2000 Declination     (dd:mm:ss.ssss)':['src_dej',str],
    'Epoch of observation (MJD)':["tstart",float],
    'Barycentered?           (1=yes, 0=no)':['barycentric',int],
    'Width of each time series bin (sec)':["tsamp",float],
    'Dispersion measure (cm-3 pc)':["refdm",float]}

# this could be expanded to begin adding support for PSRFITS
psrfits_to_sigpyproc = {
    "IBEAM":"ibeam",
    "NBITS":"nbits",
    "OBSNCHAN":"nchans",
    "SRC_NAME":"source_name",
    "RA":"src_raj",
    "DEC":"src_dej",
    "CHAN_BW":"foff",
    "TBIN":"tsamp"}

sigpyproc_to_psrfits = dict(zip(psrfits_to_sigpyproc.values(),psrfits_to_sigpyproc.keys()))

sigproc_to_tempo = {
    0:"g",
    1:"3",
    3:"f",
    4:"7",
    6:"1",
    8:"g",
    5:"8"
    }

tempo_params = ["RA","DEC","PMRA","PMDEC",
                "PMRV","BETA","LAMBDA","PMBETA",
                "PMLAMBDA","PX","PEPOCH","POSEPOCH",
                "F0","F","F1","F2","Fn",
                "P0","P","P1",
                "DM","DMn",
                "A1_n","E_n","T0_n",
                "TASC","PB_n","OM_n","FB",
                "FB_n","FBJ_n","TFBJ_n",
                "EPS1","EPS2","EPS1DOT","EPS2DOT",
                "OMDOT","OM2DOT","XOMDOT",
                "PBDOT","XPBDOT","GAMMA","PPNGAMMA",
                "SINI","MTOT","M2","DR","DTHETA",
                "XDOT","XDOT_n","X2DOT","EDOT",
                "AFAC","A0",
                "B0","BP","BPP",
                "GLEP_n","GLPH_n","GLF0_n",
                "GLF1_n","GLF0D_n","GLDT_n",
                "JUMP_n"]


def _read_char(f):
    return unpack(b"b", f.read(1))[0]


def _read_string(f):
    strlen = unpack(b"I", f.read(4))[0]
    return f.read(strlen).decode("utf-8")


def _read_int(f):
    return unpack(b"I", f.read(4))[0]


def _read_double(f):
    return unpack(b"d", f.read(8))[0]


def parseSigprocHeader(filename):
    """Parse the metadata from a Sigproc-style file header.

    @params filename: file containing the header
    :type filename: :func:`str`

    @return: observational metadata
    :rtype:  Dictionary
    """
    # f = open(filename,"r",encoding="utf-8")
    f = open(filename, "rb",)
    header = {}
    try:
        keylen = unpack(b"I", f.read(4))[0]
    except struct.error:
        raise IOError("File Header is not in sigproc format... Is file empty?")
    key = f.read(keylen)
    if key != b"HEADER_START":
        raise IOError("File Header is not in sigproc format")
    while True:
        keylen = unpack(b"I", f.read(4))[0]
        key = f.read(keylen).decode("utf-8")
        if key not in header_keys:
            print("'%s' not recognised header key") % (key)
            return None

        if header_keys[key] == "str":
            header[key] = _read_string(f)
        elif header_keys[key] == "I":
            header[key] = _read_int(f)
        elif header_keys[key] == "b":
            header[key] = _read_char(f)
        elif header_keys[key] == "d":
            header[key] = _read_double(f)
        if key == "HEADER_END":
            break

    header["hdrlen"] = f.tell()
    f.seek(0, 2)
    header["filelen"] = f.tell()
    header["nbytes"] = header["filelen"]-header["hdrlen"]
    header["nsamples"] = 8*header["nbytes"]/header["nbits"]/header["nchans"]
    f.seek(0)
    header["filename"] = filename
    header["basename"] = os.path.splitext(filename)[0]
    f.close()
    return header


def radec_to_str(val):
    """Convert Sigproc format RADEC float to a string.
    :param val: Sigproc style RADEC float (eg. 124532.123)
    :type val: float

    :returns: 'xx:yy:zz.zzz' format string
    :rtype: :func:`str`
    """
    if val < 0:
        sign = -1
    else:
        sign = 1
    fractional,integral = np.modf(abs(val))
    xx = (integral-(integral % 10000))/10000
    yy = ((integral-(integral % 100))/100)-xx*100
    zz = integral - 100*yy - 10000*xx + fractional
    zz = "%07.4f" % (zz)
    return "%02d:%02d:%s" % (sign*xx, yy, zz)

def MJD_to_Gregorian(mjd):
    """Convert Modified Julian Date to the Gregorian calender.
    :param mjd: Modified Julian Date
    :type mjd float:
    :returns: date and time
    :rtype: :func:`tuple` of :func:`str`
    """
    tt = np.fmod(mjd, 1)
    hh = tt*24.
    mm = np.fmod(hh, 1)*60.
    ss = np.fmod(mm, 1)*60.
    ss = "%08.5f" % (ss)
    j = mjd+2400000.5
    j = int(j)
    j = j - 1721119
    y = (4 * j - 1) / 146097
    j = 4 * j - 1 - 146097 * y
    d = j / 4
    j = (4 * d + 3) / 1461
    d = 4 * d + 3 - 1461 * j
    d = (d + 4) / 4
    m = (5 * d - 3) / 153
    d = 5 * d - 3 - 153 * m
    d = (d + 5) / 5
    y = 100 * y + j
    if m < 10:
        m = m + 3
    else:
        m = m - 9
        y = y + 1
    return("%02d/%02d/%02d" % (d, m, y), "%02d:%02d:%s" % (hh, mm, ss))


def rad_to_dms(rad):
    """Convert radians to (degrees, arcminutes, arcseconds)."""
    if (rad < 0.0):
        sign = -1
    else: sign = 1
    arc = (180/np.pi) * np.fmod(np.fabs(rad), np.pi)
    d = int(arc)
    arc = (arc - d) * 60.0
    m = int(arc)
    s = (arc - m) * 60.0
    if sign == -1 and d == 0:
        return (sign * d, sign * m, sign * s)
    else:
        return (sign * d, m, s)


def dms_to_rad(deg, min_, sec):
    """Convert (degrees, arcminutes, arcseconds) to radians."""
    if (deg < 0.0):
        sign = -1
    elif (deg==0.0 and (min_ < 0.0 or sec < 0.0)):
        sign = -1
    else:
        sign = 1
    return sign * (np.pi/180/60./60.) * \
        (60.0 * (60.0 * np.fabs(deg) +
                 np.fabs(min_)) + np.fabs(sec))


def dms_to_deg(deg, min_, sec):
    """Convert (degrees, arcminutes, arcseconds) to degrees."""
    return (180./np.pi) * dms_to_rad(deg, min_, sec)


def rad_to_hms(rad):
    """Convert radians to (hours, minutes, seconds)."""
    rad = np.fmod(rad, 2*np.pi)
    if (rad < 0.0): rad = rad + 2*np.pi
    arc = (12/np.pi) * rad
    h = int(arc)
    arc = (arc - h) * 60.0
    m = int(arc)
    s = (arc - m) * 60.0
    return (h, m, s)


def hms_to_rad(hour, min_, sec):
    """Convert (hours, minutes, seconds) to radians."""
    if (hour < 0.0): sign = -1
    else: sign = 1
    return sign * np.pi/12/60./60. * \
        (60.0 * (60.0 * np.fabs(hour) +
                 np.fabs(min_)) + np.fabs(sec))


def hms_to_hrs(hour, min_, sec):
    """Convert (hours, minutes, seconds) to hours."""
    return (12./np.pi) * hms_to_rad(hour, min_, sec)


def ra_to_rad(ra_string):
    """Convert right ascension string to radians."""
    h, m, s = ra_string.split(":")
    return hms_to_rad(int(h), int(m), float(s))


def dec_to_rad(dec_string):
    """Convert declination string to radians."""
    d, m, s = dec_string.split(":")
    if "-" in d and int(d)==0:
        m, s = '-'+m, '-'+s
    return dms_to_rad(int(d), int(m), float(s))


def updateHeader(header):
    """Check for changes in header and recalculate all derived quantities.
        """
    if "filename" in header:
        header["basename"], header["extension"] = splitext(header["filename"])
    if 'foff' in header and "nchans" in header and "fch1" in header:
        header["bandwidth"] = abs(header["foff"])* header["nchans"]
        header["ftop"] = header["fch1"] - 0.5*header["foff"]
        header["fbottom"] = header["ftop"] + header["foff"]*header["nchans"]
        header["fcenter"] = header["ftop"] + 0.5*header["foff"]*header["nchans"]
        header["tobs"] = header["tsamp"] * header["nsamples"]
        header["src_raj"] = header.get("src_raj", 0)
        header["src_dej"] = header.get("src_dej", 0)
        header["ra"] = radec_to_str(header["src_raj"])
        header["dec"] = radec_to_str(header["src_dej"])
        header["ra_rad"] = ra_to_rad(header["ra"])
        header["dec_rad"] = dec_to_rad(header["dec"])
        header["ra_deg"] = header["ra_rad"]*180./np.pi
        header["dec_deg"] = header["dec_rad"]*180./np.pi
    if "tstart" in header:
        header["obs_date"], header["obs_time"] = MJD_to_Gregorian(header["tstart"])
    if "nbits" in header:
        header["dtype"] = nbits_to_dtype[header["nbits"]]
    return header
