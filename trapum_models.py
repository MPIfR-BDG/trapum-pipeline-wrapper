# coding: utf-8
from sqlalchemy import CHAR, Column, Float, ForeignKey, String, TIMESTAMP, Text, text
from sqlalchemy.dialects.mysql import INTEGER, TIMESTAMP, TINYINT
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class BeamformerConfiguration(Base):
    __tablename__ = 'Beamformer_Configuration'

    bf_config_id = Column(INTEGER(11), primary_key=True, comment='beamformer configuration id')
    centre_frequency = Column(Float)
    bandwidth = Column(Float)
    incoherent_nchans = Column(INTEGER(10))
    coherent_nchans = Column(INTEGER(10))
    incoherent_tsamp = Column(Float)
    coherent_tsamp = Column(Float)
    receiver = Column(Text, comment='Receiver used')
    metadata_ = Column('metadata', Text, comment='info about coherent and incoherent config')


class FileState(Base):
    __tablename__ = 'File_States'

    state_id = Column(INTEGER(11), primary_key=True, comment='unique file state identifier')
    state = Column(Text, nullable=False, comment='description of file state')
    action = Column(Text)
    locked = Column(TINYINT(1), nullable=False, server_default=text("'0'"))
    note = Column(Text)


class FileType(Base):
    __tablename__ = 'File_Types'

    file_type_id = Column(INTEGER(11), primary_key=True)
    name = Column(Text, nullable=False)
    description = Column(Text)


class Hardware(Base):
    __tablename__ = 'Hardwares'

    hardware_id = Column(INTEGER(11), primary_key=True, comment='Hardware ID')
    name = Column(Text)
    metadata_ = Column('metadata', Text)
    notes = Column(Text, nullable=False)


class Pipeline(Base):
    __tablename__ = 'Pipelines'

    pipeline_id = Column(INTEGER(11), primary_key=True, comment='Pipeline ID')
    hash = Column(Text, comment='unique hash of pipeline')
    name = Column(Text, nullable=False, comment='Name of pipeline')
    notes = Column(Text, comment='Notes about pipeline')


class Project_(Base):
    __tablename__ = 'Projects'

    project_id = Column(INTEGER(11), primary_key=True, comment='Project ID')
    Name = Column(Text)
    notes = Column(Text, nullable=False, comment='useful points to be noted')


class Processing(Base):
    __tablename__ = 'Processings'

    processing_id = Column(INTEGER(11), primary_key=True, comment='unique ID of processing')
    pipeline_id = Column(ForeignKey('Pipelines.pipeline_id', onupdate='CASCADE'), nullable=False, index=True, comment='unique ID of pipeline through which data is running')
    hardware_id = Column(ForeignKey('Hardwares.hardware_id', onupdate='CASCADE'), index=True, comment='unique hardware iderntifier')
    submit_time = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"))
    start_time = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    end_time = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    process_status = Column(Text, comment='status of processing')
    metadata_ = Column('metadata', Text, comment='some extra parameters if any')
    notes = Column(Text, comment='notes about processing')

    hardware = relationship('Hardware')
    pipeline = relationship('Pipeline')


class Target(Base):
    __tablename__ = 'Targets'

    target_id = Column(INTEGER(11), primary_key=True, comment='unqiue ID of target')
    project_id = Column(ForeignKey('Projects.project_id', onupdate='CASCADE'), nullable=False, index=True, comment='unique project name identifer')
    source_name = Column(Text, comment='Name of source')
    ra = Column(CHAR(20))
    dec = Column(CHAR(20))
    region = Column(CHAR(20))
    semi_major_axis = Column(Float, nullable=False, server_default=text("'0'"), comment='length of semi major axis of elliptic target region')
    semi_minor_axis = Column(Float, nullable=False, server_default=text("'0'"), comment='length of semi minor axis of elliptic target region')
    position_angle = Column(Float, nullable=False, server_default=text("'0'"), comment='angle of source system with respect to plane of sky; edge on is 90 deg')
    metadata_ = Column('metadata', CHAR(20))
    notes = Column(Text, nullable=False, comment='useful points to be noted')

    project = relationship('Project_')


class Pointing(Base):
    __tablename__ = 'Pointings'

    pointing_id = Column(INTEGER(11), primary_key=True, comment='unique pointing id')
    target_id = Column(ForeignKey('Targets.target_id', onupdate='CASCADE'), nullable=False, index=True, comment='pointing identifier')
    bf_config_id = Column(ForeignKey('Beamformer_Configuration.bf_config_id', onupdate='CASCADE'), nullable=False, index=True, comment='subarray identifier')
    tobs = Column(Float, comment='Observation time (s)')
    utc_start = Column(TIMESTAMP(fsp=6), nullable=False, server_default=text("CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)"))
    sb_id = Column(Text, nullable=False, comment='scheduling block ID')
    mkat_pid = Column(Text, nullable=False, comment='MeerKAT project ID used for schedule blocks')
    metadata_ = Column('metadata', Text)
    notes = Column(Text)
    beam_shape = Column(Text, comment='coherent beam parameters')

    bf_config = relationship('BeamformerConfiguration')
    target = relationship('Target')


class Beam(Base):
    __tablename__ = 'Beams'

    beam_id = Column(INTEGER(11), primary_key=True, comment='unique beam identifier')
    pointing_id = Column(ForeignKey('Pointings.pointing_id', onupdate='CASCADE'), nullable=False, index=True)
    on_target = Column(TINYINT(1), nullable=False, comment='Indicate on and off axis beams: 1 yes , 0 no')
    ra = Column(String(255))
    decl = Column(String(255))
    coherent = Column(TINYINT(1), comment=' Coherent or incoherent beam')
    beam_name = Column(String(100))

    pointing = relationship('Pointing')


class DataProduct(Base):
    __tablename__ = 'Data_Products'

    dp_id = Column(INTEGER(11), primary_key=True, index=True, comment='unique derivative products ID')
    pointing_id = Column(ForeignKey('Pointings.pointing_id', onupdate='CASCADE'), nullable=False, index=True)
    beam_id = Column(ForeignKey('Beams.beam_id', onupdate='CASCADE'), nullable=False, index=True, comment='beam identifer')
    processing_id = Column(INTEGER(11), comment='unique ID of processing it created')
    state_id = Column(ForeignKey('File_States.state_id', onupdate='CASCADE'), nullable=False, index=True)
    filename = Column(String(50), nullable=False, comment='file basename')
    filepath = Column(String(255))
    filehash = Column(String(100))
    metainfo = Column(Text)
    file_type_id = Column(ForeignKey('File_Types.file_type_id'), nullable=False, index=True, comment='file type identifier')
    upload_date = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='Upload date of dataproduct info in db')
    modification_date = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"))

    beam = relationship('Beam')
    file_type = relationship('FileType')
    pointing = relationship('Pointing')
    state = relationship('FileState')


class ProcessingPivot(Base):
    __tablename__ = 'Processing_Pivot'

    processing_pivot_id = Column(INTEGER(11), primary_key=True, comment='unique pivot id')
    dp_id = Column(ForeignKey('Data_Products.dp_id', onupdate='CASCADE'), nullable=False, index=True, comment='dataproduct_id')
    processing_id = Column(ForeignKey('Processings.processing_id', onupdate='CASCADE'), nullable=False, index=True, comment='processing identifier')

    dp = relationship('DataProduct')
    processing = relationship('Processing')

