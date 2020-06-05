import sys
import glob
import os
import numpy as np
import HeaderParams as conf
from datetime import datetime
import struct
from struct import unpack
from os.path import splitext


def _read_char(f):
    return unpack(b"b",f.read(1))[0]

def _read_string(f):
    strlen = unpack(b"I",f.read(4))[0]
    return f.read(strlen).decode("utf-8")

def _read_int(f):
    return unpack(b"I",f.read(4))[0]

def _read_double(f):
    return unpack(b"d",f.read(8))[0]


def parseSigprocHeader(filename):
    """Parse the metadata from a Sigproc-style file header.

    @params filename: file containing the header
    :type filename: :func:`str`
    
    @return: observational metadata
    :rtype:  Dictionary
    """
    #f = open(filename,"r",encoding="utf-8")
    f = open(filename,"rb",)
    header = {}
    try:
        keylen = unpack(b"I",f.read(4))[0]
    except struct.error:
        raise IOError("File Header is not in sigproc format... Is file empty?")
    key = f.read(keylen)
    if key != b"HEADER_START":
        raise IOError("File Header is not in sigproc format")
    while True:
        keylen = unpack(b"I",f.read(4))[0]
        key = f.read(keylen).decode("utf-8")
        if not key in conf.header_keys:
            print("'%s' not recognised header key")%(key)
            return None

        if conf.header_keys[key] == "str":
            header[key] = _read_string(f)
        elif conf.header_keys[key] == "I":
            header[key] = _read_int(f)
        elif conf.header_keys[key] == "b":
            header[key] = _read_char(f)
        elif conf.header_keys[key] == "d":
            header[key] = _read_double(f)
        if key == "HEADER_END":
            break

    header["hdrlen"] = f.tell()
    f.seek(0,2)
    header["filelen"]  = f.tell()
    header["nbytes"] =  header["filelen"]-header["hdrlen"]
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
    xx = (integral-(integral%10000))/10000
    yy = ((integral-(integral%100))/100)-xx*100
    zz = integral - 100*yy - 10000*xx + fractional
    zz = "%07.4f"%(zz)
    return "%02d:%02d:%s"%(sign*xx,yy,zz)

def MJD_to_Gregorian(mjd):
    """Convert Modified Julian Date to the Gregorian calender.
    :param mjd: Modified Julian Date
    :type mjd float:
    :returns: date and time
    :rtype: :func:`tuple` of :func:`str`
    """
    tt = np.fmod(mjd,1)
    hh = tt*24.
    mm = np.fmod(hh,1)*60.
    ss = np.fmod(mm,1)*60.
    ss = "%08.5f"%(ss)
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
    return("%02d/%02d/%02d"%(d,m,y),"%02d:%02d:%s"%(hh,mm,ss))

def rad_to_dms(rad):
    """Convert radians to (degrees, arcminutes, arcseconds)."""
    if (rad < 0.0): sign = -1
    else: sign = 1
    arc = (180/np.pi) * np.fmod(np.fabs(rad),np.pi)
    d = int(arc)
    arc = (arc - d) * 60.0
    m = int(arc)
    s = (arc - m) * 60.0
    if sign==-1 and d==0:
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
       header["bandwidth"]  = abs(header["foff"])* header["nchans"]
       header["ftop"]       = header["fch1"]     - 0.5*header["foff"]
       header["fbottom"]    = header["ftop"]     + header["foff"]*header["nchans"]
       header["fcenter"]    = header["ftop"]     + 0.5*header["foff"]*header["nchans"]
            # If fch1 is the frequency of the middle of the top 
            # channel and foff negative, this is fine.
            # However, if fch1 the frequency of the middle of the bottom channel and foff 
            # positive you should run an Filterbank.Filterbank.invertFreq on the data
       header["tobs"]    = header["tsamp"] * header["nsamples"]
       header["src_raj"] = header.get("src_raj",0)
       header["src_dej"] = header.get("src_dej",0)
       header["ra"]      = radec_to_str(header["src_raj"])
       header["dec"]     = radec_to_str(header["src_dej"])
       header["ra_rad"]  = ra_to_rad(header["ra"])
       header["dec_rad"] = dec_to_rad(header["dec"])
       header["ra_deg"]  = header["ra_rad"]*180./np.pi
       header["dec_deg"] = header["dec_rad"]*180./np.pi

        
    if "tstart" in header:
        header["obs_date"],header["obs_time"] = MJD_to_Gregorian(header["tstart"])
                
    if "nbits" in header:
        header["dtype"] = conf.nbits_to_dtype[header["nbits"]]

    return header



if __name__=='__main__':


    new_paths = np.array(['/beegfs/u/prajwalvp/filterbanks/M30/dir1','/beegfs/u/prajwalvp/filterbanks/ngc6440/filterbanks'])   ### For testing only   
#############Testing##############################


    
    for new_path in new_paths:


        all_filterbanks = sorted(glob.glob(new_path+'/*.fil'))
        utc_list=[]
        utc_beam_name_list=[]
        

        for filterbank in all_filterbanks:

            # Validate header
            print(filterbank)
            
            #testing am_list = [am_info['coherent_nchans'],"{0:.8f}".format(float(am_info['coherent_tsamp'])),am_info['source_name']]
             
            file_info = parseSigprocHeader(filterbank)          
            file_info = updateHeader(file_info)     
            print (len(file_info.keys()))
