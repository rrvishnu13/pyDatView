import numpy as np
import h5py
import pandas as pd
from file import File


class SimaH5(File):

    @staticmethod
    def defaultExtensions():
        return ['.h5']

    @staticmethod
    def formatName():
        return 'SIMA H5 file'

    def __init__(self,filename=None,**kwargs):
        self.filename = filename
        if filename:
            self.read(**kwargs)


    def _read(self):
        """ use pandas read_parquet function to read parquet file"""
        self.data=h52pq(self.filename)

    def _write(self):
        """ use pandas DataFrame.to_parquet method to write parquet file """
        self.data.to_parquet(path=self.filename)

    def toDataFrame(self):
        #already stored as a data frame in self.data
        #just return self.data
        return self.data

    def fromDataFrame(self, df):
        #data already in dataframe
        self.data = df

    def toString(self):
        """ use pandas DataFrame.to_string method to convert to a string """
        s=self.data.to_string()
        return s

    @property
    def channels(self):
        if self.data is None:
            return []
        def no_unit(s):
            # s=s.replace('(','[').replace(')',']').replace(' [','_[').strip(']')
            s = s.strip(']')
            try:
                return s.split('_[')[0].strip()
            except:
                return s.strip()
        channels = [no_unit(c) for c in self.data.columns]
        return channels

    @property
    def units(self):
        if self.data is None:
            return []
        def unit(s):
            # s=s.replace('(','[').replace(')',']').replace(' [','_[').strip(']')
            s = s.strip(']')
            try:
                return s.split('_[')[1].strip()
            except:
                return s.strip()
        units = [unit(c) for c in self.data.columns]
        return units

    def __repr__(self):
        s ='Class sima h5 (attributes: data)\n'
        return s



def getTimeSeries(f):
    '''Get list of all time series in the h5 file'''
    ts_metadata = []
    
    def visitor(obj_name, obj):     
        if ('Dynamic' in obj_name) and ('delta' in obj.attrs):  #shape attribute for identifying datasets, 
                                                                #delay to remove things which dont have delta - like text files
                ts_metadata.append({
                        'path': obj_name,
                        't0': np.squeeze(obj.attrs['start']),
                        'dt': np.squeeze(obj.attrs['delta']),
                        'unit': obj.attrs.get('yunit', b'').decode('utf-8').replace('*', ''),
                        'n_points' : obj.shape[0]
                    })
    f.visititems(visitor)
    return ts_metadata

def get_cond_sets(ts_metadata):
    '''
    It prefixes the condition name to the metadata - allowing to allow condition sets to exist in the
    same h5 file
    '''
    cond_sets = {}
    for item in ts_metadata:
        parts = item['path'].split('/')
        dyn_idx = parts.index('Dynamic')
        prefix = '/'.join(parts[:dyn_idx])
        cond_sets.setdefault(prefix, []).append(item)
    
    return cond_sets

def get_common_time_index(items):
    #Creates a common time index for all time series
    dt_min = min(item['dt'] for item in items) #Smallest dt determines the resolution
    t0_min = min(item['t0'] for item in items) 
    t_end_max = max(item['t0'] + (item['n_points'] - 1) * item['dt'] for item in items)
    t_common = np.arange(t0_min, t_end_max + (dt_min / 2), dt_min)
    return t_common

def interp_series(t_common, ts_meta, f):
    '''Linear interpolation to common time index'''
    t_orig = ts_meta['t0'] + np.arange(ts_meta['n_points']) * ts_meta['dt']
    s_orig = f[ts_meta['path']][()]
    s_interp = np.interp(t_common, t_orig, s_orig, left=s_orig[0], right=s_orig[-1])
    return s_interp


def h52pq(h5File):

    with h5py.File(h5File, "r") as f:
        
        datasets = getTimeSeries(f) # get list of all time series in the h5 file with their metadata

        cond_sets = get_cond_sets(datasets) #key is the name of the condition, value is the ts attributes
        if len(cond_sets) > 1:
            raise Exception(f"Expected a single condition set in '{h5File}', found {len(cond_sets)}: {list(cond_sets.keys())}")
        
        prefix = list(cond_sets.keys())[0]
        items = cond_sets[prefix]
        t_common = get_common_time_index(items)
        group_data = {'Time_[s]': t_common}
        
        for ts_meta in items: #Process each time series in a given condition set
            key_name = ts_meta['path'].split('Dynamic/')[-1] + f"_[{ts_meta['unit']}]"
            group_data[key_name] =interp_series(t_common, ts_meta, f)
        
        df = pd.DataFrame(group_data)
         
    return df


   