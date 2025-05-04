import ctypes as C
import os
import struct
from loguru import logger

from .IIAPI_CONSTANTS import *


##  the following are known from iiapidep.h
II_BOOL = C.c_int  ##  II_BOOL is not what you might guess...
II_CHAR = C.c_char
II_FLOAT4 = C.c_float
II_FLOAT8 = C.c_double
II_INT = C.c_int
II_INT1 = C.c_char
II_INT2 = C.c_short
II_INT4 = C.c_int
II_INT8 = C.c_longlong
II_LONG = C.c_int
II_PTR = C.c_void_p
II_STR = C.c_char_p  ##  use for e.g. II_CHAR II_FAR *string
II_UCHAR = C.c_ubyte
II_UINT1 = C.c_ubyte
II_UINT2 = C.c_ushort
II_UINT4 = C.c_uint
II_UINT8 = C.c_ulonglong
II_ULONG = C.c_uint

IIAPI_DT_ID = II_INT2
IIAPI_QUERYTYPE = II_ULONG
IIAPI_STATUS = II_ULONG


##  defining the IIAPI_DEV_MODE envar puts the API in development mode
if 'IIAPI_DEV_MODE' in os.environ:
    IIAPI_DEV_MODE = True
else:
    IIAPI_DEV_MODE = False


class IIAPI(C.Structure):
    '''structures for the Ingres OpenAPI'''

    if IIAPI_DEV_MODE:
        ##  this checking is only useful during development
        def __setattr__(self, name, value):
            '''prevent undeclared attributes from being set'''
            names = [name for name, type in self._fields_]
            if name not in names:
                struct_name = self.__class__.__name__
                diagnostic = '{struct_name} object has no attribute "{name}"'
                raise AttributeError(diagnostic)
            super().__setattr__(name, value)

    display_trim = ''


    ##  all the OpenAPI structure member names have a structure-specific
    ##  prefix (e.g. co_genParm, qy_genparm, etc). The following methods
    ##  facilitate access when the prefix is not known, using only the
    ##  suffix (e.g. genParm)

    def _field_name_prefix(self):
        '''return the field name prefix used in the structure'''

        prefix = None
        field = self._fields_[0]
        name = field[0]
        prefix = name[0:2]
        return prefix


    def field_by_suffix(self,suffix):
        '''get a structure field using only its name-suffix'''

        prefix = self._field_name_prefix()
        full_name = prefix + '_' + suffix
        try:
            field = getattr(self,full_name)
        except AttributeError:
            field = None
        return field


    def genParm(self):
        '''return the genParm field (if any)'''

        return self.field_by_suffix('genParm')


    def connHandle(self):
        '''return the connHandle field (if any)'''

        return self.field_by_suffix('connHandle')


    def tranHandle(self):
        '''return the tranHandle field (if any)'''

        return self.field_by_suffix('tranHandle')


    def stmtHandle(self):
        '''return the stmtHandle field (if any)'''

        return self.field_by_suffix('stmtHandle')


    def __repr__(self):
        struct_name = self.__class__.__name__
        repr = []
        repr.append('')
        repr.append(f'{struct_name}:')
        for name, type in self._fields_:
            value = getattr(self, name)
            if type == II_PTR:
                handle = f'{value:#018x}' if value else 'None'
                repr.append(f'{name}={handle}')
            else:
                repr.append(f'{name}={value}')
        separator = '\n' + self.display_trim
        return separator.join(repr)


class IIAPI_MEMBER(IIAPI):
    '''data structures used as members by the OpenAPI'''

    display_trim = '||  '


#class IIAPI_GENERIC(object):
#    '''mixin class to return *_genParm, *_connHandle, etc'''

class IIAPI_GENPARM(IIAPI_MEMBER):
    '''common parameter member for Ingres OpenAPI'''

    _fields_ = [
        ('gp_callback', II_PTR),
        ('gp_closure', II_PTR),
        ('gp_completed', II_BOOL),
        ('gp_status', IIAPI_STATUS),
        ('gp_errorHandle', II_PTR),
    ]


class IIAPI_DESCRIPTOR(IIAPI_MEMBER):
    '''The IIAPI_DESCRIPTOR data type describes OpenAPI data, including
    its type, length, precision, scale, and usage.'''

    _fields_ = [
        ('ds_dataType', IIAPI_DT_ID),
        ('ds_nullable', II_BOOL),
        ('ds_length', II_UINT2),
        ('ds_precision', II_INT2),
        ('ds_scale', II_INT2),
        ('ds_columnType', II_INT2),
        ('ds_columnName', II_STR),
    ]


class IIAPI_DATAVALUE(IIAPI_MEMBER):
    '''generic column data'''

    _fields_ = [
        ('dv_null', II_BOOL), 
        ('dv_length', II_UINT2), 
        ('dv_value', II_PTR)
    ]


class IIAPI_FDATADESCR(IIAPI_MEMBER):
    '''
    This data type describes the data in a copy file. It also describes
    how the file should be formatted.
    '''

    _fields_ = [
        ('fd_name', C.c_char_p),
        ('fd_type', C.c_short),
        ('fd_length', C.c_short),
        ('fd_prec', C.c_short),
        ('fd_column', C.c_int),
        ('fd_funcID', C.c_int),
        ('fd_cvLen', C.c_int),
        ('fd_cvPrec', C.c_int),
        ('fd_delimiter', C.c_bool),
        ('fd_delimLength', C.c_short),
        ('fd_delimValue', C.c_char_p),
        ('fd_nullable', C.c_bool),
        ('fd_nullInfo', C.c_bool),
        ('fd_nullDescr', IIAPI_DESCRIPTOR),
        ('fd_nullValue', IIAPI_DATAVALUE),
    ]


class IIAPI_COPYMAP(IIAPI_MEMBER):
    '''
    The IIAPI_COPYMAP data type provides information needed
    to execute the copy statement, including the copy file name,
    log file name, number of columns in a row, and a description
    of the data.
    '''

    _fields_ = [
        ('cp_copyFrom', II_BOOL),
        ('cp_flags', II_ULONG),
        ('cp_errorCount', II_LONG),
        ('cp_fileName', II_STR),
        ('cp_logName', II_STR),
        ('cp_dbmsCount', II_INT2),
        ('cp_dbmsDescr', C.POINTER(IIAPI_DESCRIPTOR)),
        ('cp_fileCount', II_INT2),
        ('cp_fileDescr', C.POINTER(IIAPI_FDATADESCR)),
    ]


class IIAPI_SVR_ERRINFO(IIAPI_MEMBER):
    '''additional server information associated with error messages'''

    # svr_severity (from iiapi.h):
    # define IIAPI_SVR_DEFAULT	0x0000
    # define IIAPI_SVR_MESSAGE	0x0001
    # define IIAPI_SVR_WARNING	0x0002
    # define IIAPI_SVR_FORMATTED	0x0010

    _fields_ = [
        ('svr_id_error', II_LONG),
        ('svr_local_error', II_LONG),
        ('svr_id_server', II_LONG),
        ('svr_server_type', II_LONG),
        ('svr_severity', II_LONG),
        ('svr_parmCount', II_INT2),
        ('svr_parmDescr', C.POINTER(IIAPI_DESCRIPTOR)),
        ('svr_parmValue', C.POINTER(IIAPI_DATAVALUE)),
    ]


class IIAPI_II_TRAN_ID(IIAPI_MEMBER):
    '''Ingres transaction ID'''

    _fields_ = [
        ('it_highTran', II_UINT4), 
        ('it_lowTran', II_UINT4)
    ]


class IIAPI_II_DIS_TRAN_ID(IIAPI_MEMBER):
    '''"conventional" 2PC transaction identifier'''

    _fields_ = [
        ('ii_tranID', IIAPI_II_TRAN_ID),
        ('ii_tranName', II_CHAR * IIAPI_TRAN_MAXNAME),
    ]


class IIAPI_XA_TRAN_ID(IIAPI_MEMBER):
    '''XA 2PC transaction ID'''

    _fields_ = [
        ('xt_formatID', II_LONG),
        ('xt_gtridLength', II_LONG),
        ('xt_bqualLength', II_LONG),
        ('xt_data', II_CHAR * IIAPI_XA_XIDDATASIZE),
    ]


class IIAPI_XA_DIS_TRAN_ID(IIAPI_MEMBER):
    '''XA 2PC transaction identifier'''

    _fields_ = [
        ('xa_tranID', IIAPI_XA_TRAN_ID),
        ('xa_branchSeqnum', II_INT4),
        ('xa_branchFlag', II_INT4),
    ]


##  NB this is a union not a struct
class IIAPI_2PC_TRAN_ID(C.Union):
    '''2PC transaction identifier'''

    _fields_ = [
        ('iiXID', IIAPI_II_DIS_TRAN_ID), 
        ('xaXID', IIAPI_XA_DIS_TRAN_ID)
    ]


class IIAPI_TRAN_ID(IIAPI_MEMBER):
    '''
    The IIAPI_TRAN_ID data type specifies and names an OpenAPI
    transaction
    '''

    _fields_ = [
        ('ti_type', II_ULONG), 
        ('ti_value', IIAPI_2PC_TRAN_ID)
    ]


class IIAPI_PARM(IIAPI):
    '''parameter block for the Ingres OpenAPI'''

    ##  I suspect, but don't yet know for sure, that PARM blocks should
    ##  be treated as context managers; they don't do anything (yet) but
    ##  let's make them compliant at least.

    ##  I am having a bit of a crisis of faith over doing this in the
    ##  basic bindings; it should be done in the layer that makes the
    ##  API "python friendly", but I'm leaving it here for now

    def __enter__(self):
        pass

    def __exit__(self):
        pass


class IIAPI_ABORTPARM(IIAPI_PARM):
    '''parameter block for IIapi_abort'''

    _fields_ = [
        ('ab_genParm', IIAPI_GENPARM), 
        ('ab_connHandle', C.c_void_p)
    ]


class IIAPI_AUTOPARM(IIAPI_PARM):
    '''parameter block for IIapi_autocommit'''

    _fields_ = [
        ('ac_genParm', IIAPI_GENPARM),
        ('ac_connHandle', C.c_void_p),
        ('ac_tranHandle', C.c_void_p),
    ]


class IIAPI_BATCHPARM(IIAPI_PARM):
    '''parameter block for IIapi_batch'''

    _fields_ = [
        ('ba_genParm', IIAPI_GENPARM),
        ('ba_connHandle', II_PTR),
        ('ba_queryType', IIAPI_QUERYTYPE),
        ('ba_queryText', II_STR),
        ('ba_parameters', II_BOOL),
        ('ba_flags', II_UINT4),
        ('ba_tranHandle', II_PTR),
        ('ba_stmtHandle', II_PTR),
    ]


class IIAPI_CANCELPARM(IIAPI_PARM):
    '''parameter block for cancelling a query'''

    _fields_ = [('cn_genParm', IIAPI_GENPARM), ('cn_stmtHandle', II_PTR)]


class IIAPI_CATCHEVENTPARM(IIAPI_PARM):
    '''parameter block for IIapi_catchEvent'''

    _fields_ = [
        ('ce_genParm', IIAPI_GENPARM),
        ('ce_connHandle', II_PTR),
        ('ce_selectEventName', II_STR),
        ('ce_selectEventOwner', II_STR),
        ('ce_eventHandle', II_PTR),
        ('ce_eventName', II_STR),
        ('ce_eventOwner', II_STR),
        ('ce_eventDB', II_STR),
        ('ce_eventTime', IIAPI_DATAVALUE),
        ('ce_eventInfoAvail', II_BOOL),
    ]


class IIAPI_CLOSEPARM(IIAPI_PARM):
    '''parameter block for closing a query'''

    _fields_ = [('cl_genParm', IIAPI_GENPARM), ('cl_stmtHandle', II_PTR)]


class IIAPI_COMMITPARM(IIAPI_PARM):
    '''parameter block for IIapi_commit'''

    _fields_ = [('cm_genParm', IIAPI_GENPARM), ('cm_tranHandle', II_PTR)]


class IIAPI_CONNPARM(IIAPI_PARM):
    '''parameter block for IIapi_connect'''

    _fields_ = [
        ('co_genParm', IIAPI_GENPARM),
        ('co_target', II_STR),
        ('co_username', II_STR),
        ('co_password', II_STR),
        ('co_timeout', II_LONG),
        ('co_connHandle', II_PTR),
        ('co_tranHandle', II_PTR),
        ('co_sizeAdvise', II_LONG),
        ('co_apiLevel', II_LONG),
        ('co_type', II_LONG),
    ]


class IIAPI_CONVERTPARM(IIAPI_PARM):
    '''parameter block for IIapi_convertData'''

    _fields_ = [
        ('cv_srcDesc', IIAPI_DESCRIPTOR),
        ('cv_srcValue', IIAPI_DATAVALUE),
        ('cv_dstDesc', IIAPI_DESCRIPTOR),
        ('cv_dstValue', IIAPI_DATAVALUE),
        ('cv_status', IIAPI_STATUS),
    ]


class IIAPI_DISCONNPARM(IIAPI_PARM):
    '''parameter block for disconnecting from a server'''

    _fields_ = [('dc_genParm', IIAPI_GENPARM), ('dc_connHandle', II_PTR)]


class IIAPI_EVENTPARM(IIAPI_PARM):
    '''parameter block for setting up an event handler'''

    _fields_ = [
        ('ev_envHandle', II_PTR),
        ('ev_connHandle', II_PTR),
        ('ev_eventName', II_STR),
        ('ev_eventOwner', II_STR),
        ('ev_eventDB', II_STR),
        ('ev_eventTime', IIAPI_DATAVALUE),
    ]


class IIAPI_FORMATPARM(IIAPI_PARM):
    '''parameter block for IIapi_formatData'''

    _fields_ = [
        ('fd_envHandle', II_PTR),
        ('fd_srcDesc', IIAPI_DESCRIPTOR),
        ('fd_srcValue', IIAPI_DATAVALUE),
        ('fd_dstDesc', IIAPI_DESCRIPTOR),
        ('fd_dstValue', IIAPI_DATAVALUE),
        ('fd_status', IIAPI_STATUS),
    ]


class IIAPI_GETCOLPARM(IIAPI_PARM):
    '''parameter block for IIapi_getColumns'''

    _fields_ = [
        ('gc_genParm', IIAPI_GENPARM),
        ('gc_stmtHandle', II_PTR),
        ('gc_rowCount', II_INT2),  # incorrectly documented II_INT
        ('gc_columnCount', II_INT2),  # incorrectly documented II_INT
        ('gc_columnData', C.POINTER(IIAPI_DATAVALUE)),
        ('gc_rowsReturned', II_INT2),  # incorrectly documented II_INT
        ('gc_moreSegments', II_BOOL),
    ]


class IIAPI_GETCOLINFOPARM(IIAPI_PARM):
    '''parameter block for IIapi_getColumnInfo'''

    _fields_ = [
        ('gi_stmtHandle', II_PTR),
        ('gi_columnNumber', II_INT2),
        ('gi_status', IIAPI_STATUS),
        ('gi_mask', II_ULONG),
        ('gi_lobLength', II_UINT8),
    ]


class IIAPI_GETCOPYMAPPARM(IIAPI_PARM):
    '''parameter block for setting connection parameters'''

    _fields_ = [
        ('gm_genParm', IIAPI_GENPARM),
        ('gm_stmtHandle', II_PTR),
        ('gm_copyMap', IIAPI_COPYMAP),
    ]


class IIAPI_GETDESCRPARM(IIAPI_PARM):
    '''query result descriptor block'''

    _fields_ = [
        ('gd_genParm', IIAPI_GENPARM),
        ('gd_stmtHandle', II_PTR),
        ('gd_descriptorCount', II_INT2),
        ('gd_descriptor', C.POINTER(IIAPI_DESCRIPTOR)),
    ]


class IIAPI_GETEINFOPARM(IIAPI_PARM):
    '''OpenAPI parameter block for returning error and user-defined info'''

    _fields_ = [
        ('ge_errorHandle', II_PTR),
        ('ge_type', II_LONG),
        ('ge_SQLSTATE', II_CHAR * (II_SQLSTATE_LEN + 1)),
        ('ge_errorCode', II_LONG),
        ('ge_message', II_STR),
        ('ge_serverInfoAvail', II_BOOL),
        ('ge_serverInfo', C.POINTER(IIAPI_SVR_ERRINFO)),
        ('ge_status', IIAPI_STATUS),
    ]


class IIAPI_GETEVENTPARM(IIAPI_PARM):
    '''parameter block for IIapi_getEvent'''

    _fields_ = [
        ('gv_genParm', IIAPI_GENPARM),
        ('gv_connHandle', C.c_void_p),
        ('gv_timeout', C.c_int),
    ]


class IIAPI_GETQINFOPARM(IIAPI_PARM):
    '''parameter block for IIapi_getQueryInfo()'''

    _fields_ = [
        ('gq_genParm', IIAPI_GENPARM),
        ('gq_stmtHandle', II_PTR),
        ('gq_flags', II_ULONG),
        ('gq_mask', II_ULONG),
        ('gq_rowCount', II_LONG),
        ('gq_readonly', II_BOOL),
        ('gq_procedureReturn', II_LONG),
        ('gq_procedureHandle', II_PTR),
        ('gq_repeatQueryHandle', II_PTR),
        ('gq_tableKey', II_CHAR * IIAPI_TBLKEYSZ),
        ('gq_objectKey', II_CHAR * IIAPI_OBJKEYSZ),
        ('gq_cursorType', II_ULONG),
        ('gq_rowStatus', II_ULONG),
        (
            'gq_rowPosition',
            II_LONG,
        ),
        ('gq_rowCountEx', II_INT8),
    ]


class IIAPI_INITPARM(IIAPI_PARM):
    '''parameter block for initializing the Ingres OpenAPI'''

    _fields_ = [
        ('in_timeout', II_LONG),
        ('in_version', II_LONG),
        ('in_status', IIAPI_STATUS),
        ('in_envHandle', II_PTR),
    ]


class IIAPI_MODCONNPARM(IIAPI_PARM):
    '''parameter block for IIapi_modifyConnect()'''

    _fields_ = [('mc_genParm', IIAPI_GENPARM), ('mc_connHandle', II_PTR)]


class IIAPI_POSPARM(IIAPI_PARM):
    '''parameter block for IIapi_position()'''

    _fields_ = [
        ('po_genParm', IIAPI_GENPARM),
        ('po_stmtHandle', II_PTR),
        ('po_reference', II_UINT2),
        ('po_offset', II_INT),
        ('po_rowCount', II_INT2),
    ]


class IIAPI_PREPCMTPARM(IIAPI_PARM):
    '''parameter block for IIapi_prepareCommit()'''

    _fields_ = [('pr_genParm', IIAPI_GENPARM), ('pr_tranHandle', II_PTR)]


class IIAPI_PROMPTPARM(IIAPI_PARM):
    '''parameter block for prompt and tracing callbacks'''

    _fields_ = [
        ('pd_envHandle',II_PTR),
        ('pd_connHandle',II_PTR),
        ('pd_flags',II_LONG),
        ('pd_timeout',II_LONG),
        ('pd_msg_len',II_UINT2),
        ('pd_message',II_STR),
        ('pd_max_reply',II_UINT2),
        ('pd_rep_flags',II_LONG),
        ('pd_rep_len',II_UINT2),
        ('pd_reply',II_STR),
    ]


class IIAPI_PUTCOLPARM(IIAPI_PARM):
    '''parameter block for IIapi_putColumns()'''

    _fields_ = [
        ('pc_genParm', IIAPI_GENPARM),
        ('pc_stmtHandle', II_PTR),
        ('pc_columnCount', II_INT2),
        ('pc_columnData', C.POINTER(IIAPI_DATAVALUE)),
        ('pc_moreSegments', II_BOOL),
    ]


class IIAPI_PUTPARMPARM(IIAPI_PARM):
    '''parameter block for IIapi_putParms()'''

    _fields_ = [
        ('pp_genParm', IIAPI_GENPARM),
        ('pp_stmtHandle', II_PTR),
        ('pp_parmCount', II_INT2),
        ('pp_parmData', C.POINTER(IIAPI_DATAVALUE)),
        ('pp_moreSegments', II_BOOL),
    ]


class IIAPI_QUERYPARM(IIAPI_PARM):
    '''parameter block for IIapi_query()'''

    _fields_ = [
        ('qy_genParm', IIAPI_GENPARM),
        ('qy_connHandle', II_PTR),
        ('qy_queryType', IIAPI_QUERYTYPE),
        ('qy_queryText', II_STR),
        ('qy_parameters', II_BOOL),
        ('qy_tranHandle', II_PTR),
        ('qy_stmtHandle', II_PTR),
        ('qy_flags', II_ULONG),
    ]


class IIAPI_REGXIDPARM(IIAPI_PARM):
    '''parameter block for IIapi_registerXID()'''

    _fields_ = [
        ('rg_tranID', IIAPI_TRAN_ID),
        ('rg_tranIdHandle', II_PTR),
        ('rg_status', IIAPI_STATUS),
    ]


class IIAPI_RELENVPARM(IIAPI_PARM):
    '''parameter block for IIapi_releaseEnv()'''

    _fields_ = [('re_envHandle', II_PTR), ('re_status', IIAPI_STATUS)]


class IIAPI_RELXIDPARM(IIAPI_PARM):
    '''parameter block for IIapi_releaseXID()'''

    _fields_ = [('rl_tranIdHandle', II_PTR), ('rl_status', IIAPI_STATUS)]


class IIAPI_ROLLBACKPARM(IIAPI_PARM):
    '''parameter block for IIapi_rollback()'''

    _fields_ = [
        ('rb_genParm', IIAPI_GENPARM),
        ('rb_tranHandle', II_PTR),
        ('rb_savePointHandle', II_PTR),
    ]


class IIAPI_SAVEPTPARM(IIAPI_PARM):
    '''parameter block for IIapi_savePoint()'''

    _fields_ = [
        ('sp_genParm', IIAPI_GENPARM),
        ('sp_tranHandle', II_PTR),
        ('sp_savePoint', II_STR),
        ('sp_savePointHandle', II_PTR),
    ]


class IIAPI_SCROLLPARM(IIAPI_PARM):
    '''parameter block for IIapi_scroll()'''

    _fields_ = [
        ('sl_genParm', IIAPI_GENPARM),
        ('sl_stmtHandle', II_PTR),
        ('sl_orientation', II_UINT2),
        ('sl_offset', II_INT),
    ]


class IIAPI_SETCONPRMPARM(IIAPI_PARM):
    '''parameter block for setConnectParam()'''

    _fields_ = [
        ('sc_genParm', IIAPI_GENPARM),
        ('sc_connHandle', C.c_void_p),
        ('sc_paramID', C.c_int),
        ('sc_paramValue', C.c_void_p),
    ]


class IIAPI_SETDESCRPARM(IIAPI_PARM):
    '''parameter block for IIapi_setDescriptor()'''

    _fields_ = [
        ('sd_genParm', IIAPI_GENPARM),
        ('sd_stmtHandle', II_PTR),
        ('sd_descriptorCount', II_INT2),
        ('sd_descriptor', C.POINTER(IIAPI_DESCRIPTOR)),
    ]


class IIAPI_SETENVPRMPARM(IIAPI_PARM):
    '''parameter block for IIapi_setEnvParam()'''

    _fields_ = [
        ('se_envHandle', II_PTR),
        ('se_paramID', II_LONG),
        ('se_paramValue', II_PTR),
        ('se_status', IIAPI_STATUS),
    ]


class IIAPI_TERMPARM(IIAPI_PARM):
    '''parameter block for terminating the Ingres OpenAPI'''

    _fields_ = [('tm_status', IIAPI_STATUS)]


class IIAPI_TRACEPARM(IIAPI_PARM):
    '''parameter block to set up a trace callback'''

    _fields_ = [
        ('tr_envHandle', II_PTR),
        ('tr_connHandle', II_PTR),
        ('tr_length', II_INT4),
        ('tr_message', II_STR),
    ]


class IIAPI_WAITPARM(IIAPI_PARM):
    '''parameter block for waiting'''

    _fields_ = [('wt_timeout', II_LONG), ('wt_status', IIAPI_STATUS)]


class IIAPI_XACOMMITPARM(IIAPI_PARM):
    '''parameter block for IIapi_xaCommit()'''

    _fields_ = [
        ('xc_genParm', IIAPI_GENPARM),
        ('xc_connHandle', II_PTR),
        ('xc_tranID', IIAPI_TRAN_ID),
        ('xc_flags', II_ULONG),
    ]


class IIAPI_XAENDPARM(IIAPI_PARM):
    '''parameter block for IIapi_xaEnd()'''

    _fields_ = [
        ('xe_genParm', IIAPI_GENPARM),
        ('xe_connHandle', II_PTR),
        ('xe_tranID', IIAPI_TRAN_ID),
        ('xe_flags', II_ULONG),
    ]


class IIAPI_XAPREPPARM(IIAPI_PARM):
    '''parameter block for IIapi_xaPrepare()'''

    _fields_ = [
        ('xp_genParm', IIAPI_GENPARM),
        ('xp_connHandle', II_PTR),
        ('xp_tranID', IIAPI_TRAN_ID),
        ('xp_flags', II_ULONG),
    ]


class IIAPI_XAROLLPARM(IIAPI_PARM):
    '''parameter block for IIapi_xaRollback()'''

    _fields_ = [
        ('xr_genParm', IIAPI_GENPARM),
        ('xr_connHandle', II_PTR),
        ('xr_tranID', IIAPI_TRAN_ID),
        ('xr_flags', II_ULONG),
    ]


class IIAPI_XASTARTPARM(IIAPI_PARM):
    '''parameter block for IIapi_xaStart()'''

    _fields_ = [
        ('xs_genParm', IIAPI_GENPARM),
        ('xs_connHandle', II_PTR),
        ('xs_tranID', IIAPI_TRAN_ID),
        ('xs_flags', II_ULONG),
        ('xs_tranHandle', II_PTR),
    ]
