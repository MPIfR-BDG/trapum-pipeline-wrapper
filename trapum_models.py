# coding: utf-8
from sqlalchemy import Column, DateTime, Float, ForeignKey, Index, String, Table, Text
from sqlalchemy.dialects.mysql import INTEGER, TINYINT
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class Affiliation(Base):
    __tablename__ = 'affiliation'

    id = Column(INTEGER(11), primary_key=True)
    name = Column(String(64), unique=True)
    fullname = Column(String(128), unique=True)
    address = Column(String(256))

    users = relationship('User', secondary='affiliations')


class BeamformerConfiguration(Base):
    __tablename__ = 'beamformer_configuration'

    id = Column(INTEGER(11), primary_key=True)
    centre_frequency = Column(Float)
    bandwidth = Column(Float)
    incoherent_nchans = Column(INTEGER(11))
    incoherent_tsamp = Column(Float)
    incoherent_antennas = Column(Text)
    coherent_nchans = Column(INTEGER(11))
    coherent_tsamp = Column(Float)
    coherent_antennas = Column(Text)
    configuration_authority = Column(String(64))
    receiver = Column(String(20))
    metainfo = Column(Text)


class FileAction(Base):
    __tablename__ = 'file_action'

    id = Column(INTEGER(11), primary_key=True)
    action = Column(String(64), nullable=False)
    is_destructive = Column(TINYINT(1), nullable=False)


class FileType(Base):
    __tablename__ = 'file_type'

    id = Column(INTEGER(11), primary_key=True)
    name = Column(String(64), nullable=False, unique=True)
    description = Column(Text)


class Hardware(Base):
    __tablename__ = 'hardware'

    id = Column(INTEGER(11), primary_key=True)
    name = Column(Text)
    metainfo = Column(Text)
    notes = Column(Text)


class MembershipRole(Base):
    __tablename__ = 'membership_role'

    id = Column(INTEGER(11), primary_key=True)
    name = Column(String(64), unique=True)

    users = relationship('User', secondary='membership_roles')


class Pipeline(Base):
    __tablename__ = 'pipeline'

    id = Column(INTEGER(11), primary_key=True)
    hash = Column(String(100), unique=True)
    name = Column(String(64), nullable=False, unique=True)
    arguments_json = Column(Text)
    notes = Column(Text)


class ProcessingArgument(Base):
    __tablename__ = 'processing_arguments'

    id = Column(INTEGER(11), primary_key=True)
    arguments_json = Column(Text)


class User(Base):
    __tablename__ = 'user'

    id = Column(INTEGER(11), primary_key=True)
    username = Column(String(64), unique=True)
    fullname = Column(String(64))
    email = Column(String(120), unique=True)
    password_hash = Column(String(128))
    administrator = Column(TINYINT(1))
    token = Column(String(32), unique=True)
    token_expiration = Column(DateTime)

    working_groups = relationship('WorkingGroup', secondary='memberships')


t_affiliations = Table(
    'affiliations', metadata,
    Column('user_id', ForeignKey('user.id'), primary_key=True, nullable=False),
    Column('affiliation_id', ForeignKey('affiliation.id'), primary_key=True, nullable=False, index=True)
)


t_membership_roles = Table(
    'membership_roles', metadata,
    Column('user_id', ForeignKey('user.id'), primary_key=True, nullable=False),
    Column('membership_role_id', ForeignKey('membership_role.id'), primary_key=True, nullable=False, index=True)
)

t_processing_inputs = Table(
    'processing_inputs', metadata,
    Column('dp_id', ForeignKey('data_product.id'), primary_key=True, nullable=False),
    Column('processing_id', ForeignKey('processing.id'), primary_key=True, nullable=False, index=True)
)

class Processing(Base):
    __tablename__ = 'processing'

    id = Column(INTEGER(11), primary_key=True)
    pipeline_id = Column(ForeignKey('pipeline.id', onupdate='CASCADE'), nullable=False, index=True)
    hardware_id = Column(ForeignKey('hardware.id', onupdate='CASCADE'), index=True)
    arguments_id = Column(ForeignKey('processing_arguments.id', onupdate='CASCADE'), index=True)
    submit_time = Column(DateTime)
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    process_status = Column(String(20))

    arguments = relationship('ProcessingArgument')
    hardware = relationship('Hardware')
    pipeline = relationship('Pipeline')
    processing_requests = relationship('ProcessingRequest', secondary='processing_request_processings')
    inputs = relationship('DataProduct',secondary=t_processing_inputs,lazy=True)


class ProcessingRequest(Base):
    __tablename__ = 'processing_request'

    id = Column(INTEGER(11), primary_key=True)
    user_id = Column(ForeignKey('user.id', onupdate='CASCADE'), nullable=False, index=True)
    pipeline_id = Column(ForeignKey('pipeline.id', onupdate='CASCADE'), nullable=False, index=True)
    arguments_id = Column(ForeignKey('processing_arguments.id', onupdate='CASCADE'), index=True)
    name = Column(String(64))
    dispatcher_args_json = Column(Text)
    dispatched_at = Column(DateTime)
    dispatched_to = Column(String(64))

    arguments = relationship('ProcessingArgument')
    pipeline = relationship('Pipeline')
    user = relationship('User')


class WorkingGroup(Base):
    __tablename__ = 'working_group'

    id = Column(INTEGER(11), primary_key=True)
    name = Column(String(120), unique=True)
    description = Column(String(500))
    chair_user_id = Column(ForeignKey('user.id', onupdate='CASCADE'), index=True)
    outreach_liaison_user_id = Column(ForeignKey('user.id', onupdate='CASCADE'), index=True)

    chair_user = relationship('User', primaryjoin='WorkingGroup.chair_user_id == User.id')
    outreach_liaison_user = relationship('User', primaryjoin='WorkingGroup.outreach_liaison_user_id == User.id')


t_memberships = Table(
    'memberships', metadata,
    Column('user_id', ForeignKey('user.id'), primary_key=True, nullable=False),
    Column('working_group_id', ForeignKey('working_group.id'), primary_key=True, nullable=False, index=True)
)


t_processing_request_processings = Table(
    'processing_request_processings', metadata,
    Column('processing_id', ForeignKey('processing.id'), primary_key=True, nullable=False),
    Column('processing_request_id', ForeignKey('processing_request.id'), primary_key=True, nullable=False, index=True)
)


class Project(Base):
    __tablename__ = 'project'

    id = Column(INTEGER(11), primary_key=True)
    name = Column(String(120), unique=True)
    description = Column(String(500))
    working_group_id = Column(ForeignKey('working_group.id', onupdate='CASCADE'), index=True)
    coordinator_user_id = Column(ForeignKey('user.id', onupdate='CASCADE'), index=True)

    coordinator_user = relationship('User')
    working_group = relationship('WorkingGroup')


class Target(Base):
    __tablename__ = 'target'

    id = Column(INTEGER(11), primary_key=True)
    project_id = Column(ForeignKey('project.id', onupdate='CASCADE'), index=True)
    source_name = Column(String(64))
    ra = Column(String(20))
    dec = Column(String(20))
    region = Column(String(20))
    semi_major_axis = Column(Float)
    semi_minor_axis = Column(Float)
    position_angle = Column(Float)
    metainfo = Column(Text)
    notes = Column(Text)

    project = relationship('Project')


class Pointing(Base):
    __tablename__ = 'pointing'

    id = Column(INTEGER(11), primary_key=True)
    target_id = Column(ForeignKey('target.id', onupdate='CASCADE'), nullable=False, index=True)
    bf_config_id = Column(ForeignKey('beamformer_configuration.id', onupdate='CASCADE'), nullable=False, index=True)
    observation_length = Column(Float)
    utc_start = Column(DateTime, nullable=False)
    sb_id = Column(Text, nullable=False)
    mkat_pid = Column(Text, nullable=False)
    beam_shape = Column(Text)

    bf_config = relationship('BeamformerConfiguration')
    target = relationship('Target')


class Beam(Base):
    __tablename__ = 'beam'

    id = Column(INTEGER(11), primary_key=True)
    pointing_id = Column(ForeignKey('pointing.id', onupdate='CASCADE'), nullable=False, index=True)
    on_target = Column(TINYINT(1), nullable=False)
    ra = Column(String(20))
    dec = Column(String(20))
    coherent = Column(TINYINT(1))
    name = Column(String(20))

    pointing = relationship('Pointing')
    processing_requests = relationship('ProcessingRequest', secondary='processing_request_beams')


class DataProduct(Base):
    __tablename__ = 'data_product'
    __table_args__ = (
        Index('_full_file_path', 'filename', 'filepath', unique=True),
    )

    id = Column(INTEGER(11), primary_key=True, index=True)
    pointing_id = Column(ForeignKey('pointing.id', onupdate='CASCADE'), nullable=False, index=True)
    beam_id = Column(ForeignKey('beam.id', onupdate='CASCADE'), nullable=False, index=True)
    processing_id = Column(ForeignKey('processing.id', onupdate='CASCADE'), index=True)
    file_type_id = Column(ForeignKey('file_type.id', onupdate='CASCADE'), nullable=False, index=True)
    filename = Column(String(64), nullable=False)
    filepath = Column(String(255), nullable=False)
    filehash = Column(String(100))
    available = Column(TINYINT(1), nullable=False)
    locked = Column(TINYINT(1), nullable=False)
    upload_date = Column(DateTime, nullable=False)
    modification_date = Column(DateTime, nullable=False)
    metainfo = Column(Text)

    beam = relationship('Beam')
    file_type = relationship('FileType')
    pointing = relationship('Pointing')
    processing = relationship('Processing')
    processings = relationship('Processing', secondary='processing_inputs')


t_processing_request_beams = Table(
    'processing_request_beams', metadata,
    Column('beam_id', ForeignKey('beam.id'), primary_key=True, nullable=False),
    Column('processing_request_id', ForeignKey('processing_request.id'), primary_key=True, nullable=False, index=True)
)


class FileActionRequest(Base):
    __tablename__ = 'file_action_request'

    id = Column(INTEGER(11), primary_key=True)
    data_product_id = Column(ForeignKey('data_product.id', onupdate='CASCADE'), nullable=False, index=True)
    action_id = Column(ForeignKey('file_action.id', onupdate='CASCADE'), nullable=False, index=True)
    requested_at = Column(DateTime, nullable=False)
    completed_at = Column(DateTime)
    success = Column(TINYINT(1), nullable=False)

    action = relationship('FileAction')
    data_product = relationship('DataProduct')


