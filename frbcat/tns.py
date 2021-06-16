"""Script to download FRB information from the TNS servers."""
import datetime
import glob
import numpy as np
import os
import pandas as pd
import urllib.request

import frbcat.misc as misc


class TNS():
    """docstring for TNS."""

    def __init__(self,
                 oneoffs=True,
                 repeaters=True,
                 repeat_bursts=True,
                 update='monthly',
                 path=None,
                 save=True,
                 mute=False,
                 tns_id=None,
                 tns_name=None):
        """Query TNS.
        Args:
         oneoffs (bool): Whether to include oneoffs. Defaults to True.
         repeaters (bool): Whether to include repeaters. Defaults to True.
         repeat_bursts (bool): Whether to include multiple bursts per
             repeater. Defaults to True.
         update (str): Always get a new version (True), get a new version
             once a month ('monthly'), or never get a new version (False).
             Defaults to 'monthly'.
         path (str): Directory in which to save the csv file. Defaults to
             your Downloads folder.
         save (bool): Whether to save the resulting csv file to path.
         mute (bool): Whether to mute output in the terminal.
         tns_id (int): TNS user id, required when getting a new version.
         tns_name (str): TNS user name, required when getting a new version.
        """
        self.path = path
        if path is None:
            self.path = os.path.expanduser('~') + '/Downloads/'

        self.oneoffs = oneoffs
        self.repeaters = repeaters
        self.repeat_bursts = repeat_bursts
        self.update = update
        self.save = save
        self.mute = mute
        self.tns_id = tns_id
        self.tns_name = tns_name

        self.get_data()
        self.filter(one_offs=self.oneoffs,
                    repeaters=self.repeaters,
                    repeat_bursts=self.repeat_bursts)

        self.units = {'back_end': None,
                      'barycentric_event_time': None,
                      'burst_bandwidth': 'MHz',
                      'burst_width': 'ms',
                      'burst_width_err': 'ms',
                      'dec_frac': 'frac. degrees',
                      'decl': None,
                      'decl_err': None,
                      'discovery_date': None,
                      'dm': 'pc cm-3',
                      'dm_model': None,
                      'filename': None,
                      'filetype': None,
                      'fluence': 'Jy ms',
                      'fluence_err': 'Jy ms',
                      'flux': 'Jy',
                      'flux_err': 'Jy',
                      'frac_lin_pol': None,
                      'galactic_max_dm': 'pc cm^-3',
                      'galactic_max_dm_model': None,
                      'gl_frac': 'frac. degrees',
                      'gb_frac': 'frac. degrees',
                      'group': None,
                      'host_redshift': None,
                      'inst_bandwidth': 'MHz',
                      'internal_name': None,
                      'lastmodified': None,
                      'name': None,
                      'num_channels': None,
                      'num_files': None,
                      'photometry_date': None,
                      'photometry_id': None,
                      'public_webpage': None,
                      'ra': None,
                      'ra_frac': 'frac. degrees',
                      'ra_err': None,
                      'ref_freq': 'MHz',
                      'region_filename': None,
                      'remarks': None,
                      'repeater_of_objid': None,
                      'reporter_name': None,
                      'reports_id': None,
                      'rm': 'rad m^-2',
                      'rm_err': 'rad m^-2',
                      'sampling_time': 'ms',
                      'scattering_time': 'ms',
                      'scattering_time_err': 'ms',
                      'snr': None,
                      'telescope': None,
                      'telescope_mode': None,
                      'time_received': None,
                      'tns_id': None,
                      }

    def get_data(self):
        # Check whether a copy of FRBCAT has already been downloaded
        # Ensures TNS is only queried once a month
        path = self.path + '/tns_'
        path += str(datetime.datetime.today()).split()[0][:-3]
        path += '-??.csv'
        exists = glob.glob(path)
        if self.update == 'monthly' and exists:
            self.update = False

        if self.update:
            try:
                entries = self.get_json()
                df = self.json2df(entries)
                self.clean_df(df)

                if self.save:
                    date = str(datetime.datetime.today()).split()[0]
                    path = str(self.path) + '/tns_' + str(date) + '.csv'
                    self.df.to_csv(path, index=False)

            # Unless there's no internet
            except urllib.error.URLError:
                self.update = False

        if self.update is False:
            # Find latest version of frbcat
            f = max(glob.glob(self.path + '/tns*.csv'), key=os.path.getctime)
            if not self.mute:
                misc.pprint("Using " + f.split('/')[-1])

            date_cols = ['time_received', 'barycentric_event_time',
                         'discovery_date', 'photometry_date',
                         'lastmodified']
            self.df = pd.read_csv(f, parse_dates=date_cols)

    def row2json(self, line):
        """Convert row of html table to json format."""
        out = {}
        for seg in line.split('</td>'):
            if '<td ' in seg:
                key = seg.split('<td ')[-1].split('class="cell-')[-1]
                key = key.split('"')[0]
                val = seg.split('<td ')[-1].split('>', 1)[1].strip()
                if val:
                    ks = ('filename', 'public_webpage', 'region_filename')
                    if key in ks:
                        val = val.split('href="')[1].split('"')[0]
                    if key in ('photometry', 'related_files', 'reps'):
                        val = val.split('<a')[0]
                    if key in ('id', 'name', 'repeater_of_objid'):
                        val = val.split('</a>')[0].split('>')[-1]
                    out[key] = val
                if '<' in val:
                    print(key, val)
        return out

    def get_json(self):
        entries = []
        more = True
        page = 0
        page_length = 500

        if not self.mute:
            m = 'Attempting to retrieve FRBs from the Transient Name Server'
            misc.pprint(m)

        # Provide user agent to be able to access the webpage
        if self.tns_id is None or self.tns_name is None:
            raise ValueError('Provide tns_id and tns_name arguments '
                             'when updating TNS data')
        header = {'User-Agent': str({'tns_id': self.tns_id,
                  'type': 'user', 'name': self.tns_name})}

        # Loop through pages on TNS webpage till no more results
        while more:
            # Limit results to frbs
            url = 'https://www.wis-tns.org/search?&include_frb=1'
            url += '&objtype%5B%5D=130&num_page=' + str(page_length) + '&page='
            url += str(page)

            request = urllib.request.Request(url, headers=header)
            with urllib.request.urlopen(request) as resp:
                data = resp.read().decode().split('\n')

            if not self.mute:
                misc.pprint('Succeeded')

            # Go through HTML table
            for line in data:
                if '<thead>' in line:
                    continue

                # Split out the various tables (main, photometry etc)
                if ('class="cell-reps"' in line and
                        'class="cell-ot_name"' in line):
                    entry = self.row2json(line)
                    entry['reports_list'] = []
                    entry['photometry_list'] = []
                    entry['file_list'] = []
                    entries.append(entry)
                if ('class="cell-reporter_name"' in line and
                        'class="cell-photometry"' in line):
                    entries[-1]['reports_list'].append(self.row2json(line))
                if ('class="cell-snr"' in line and
                        'class="cell-ref_freq"' in line):
                    entries[-1]['photometry_list'].append(self.row2json(line))
                if ('class="cell-filename"' in line and
                        'class="cell-filetype"' in line):
                    entries[-1]['file_list'].append(self.row2json(line))

            if entries and len(entries) % page_length == 0:
                page += 1
            else:
                more = False

        return entries

    def json2df(self, entries):

        # Create a nice list of dictionaries
        rows = []
        for frb in entries:
            row = {}
            for par in frb:
                if type(frb[par]) == list and len(frb[par]) > 0:
                    # Always take the most recent entry
                    # TODO: This might need to be updated at some stage
                    for other_par in frb[par][0]:
                        name = other_par
                        if other_par in frb:
                            name = par.split('_')[0] + '_' + other_par
                        row[name] = frb[par][0][other_par]
                else:
                    row[par] = frb[par]
            rows.append(row)

        # Convert to a DataFrame
        return pd.DataFrame(rows)

    def clean_df(self, df):

        # Remove unneccsary columns
        cols = ['reps',  # Number of reports on an FRB
                'ot_name',  # Object type (all FRBS anyway)
                'isTNS_AT',  # No idea what this parameter is for
                'public',  # All downloaded FRBs are of course public
                'user_name',  # Ensuring some privacy
                'discoverydate',  # There is already a discovery_date column
                'reports_internal_name',  # Already have internal_name
                'discoverymag',  # Same as flux column
                'file_list',  # Empty key
                'file_name',  # Copy of filename
                'ra',  # Already in reports_ra with error margin
                'decl',  # Same as above
                'dm',  # Already in reports dm with more info
                'ext_catalogs',  # External catalogues are irrelevant
                'discoverer',  # Already in reporter_name
                'observer',  # Already in reporter_name
                'photometry',  # Number of photometry options
                'end_prop_period',  # Properitory period is irrelevant
                'reports_end_prop_period',  # Same as above
                'unit_name',  # All in Jy
                'disc_filter_name'  # Same as filter_name
                ]

        if df.source_group_name.equals(df.reporting_group_name):
            cols.append('source_group_name')
        if df.source_group_name.equals(df.reports_reporting_group_name):
            cols.append('reports_reporting_group_name')
        if df.groups.equals(df.reporting_group_name):
            cols.append('reporting_group_name')
        if df.source_group_name.equals(df.reports_source_group_name):
            cols.append('reports_source_group_name')
        if df.disc_filter_name.equals(df.filter_name):
            cols.append('disc_filter_name')
        if df.galactic_max_dm.equals(df.reports_galactic_max_dm):
            cols.append('reports_galactic_max_dm')
        if df.public_webpage.equals(df.reports_public_webpage):
            cols.append('reports_public_webpage')

        df = df.drop(cols, axis=1, errors='ignore')

        # Change some names
        df.rename(columns={'filter_name': 'back_end',
                           'obsdate': 'photometry_date',
                           'groups': 'group',
                           'id': 'tns_id',
                           'related_files': 'num_files',
                           'channels_no': 'num_channels'}, inplace=True)

        # Set dtypes
        df = df.astype({'tns_id': int,
                        'name': str,
                        'galactic_max_dm': str,
                        'reports_id': int,
                        'photometry_id': int,
                        'snr': float,
                        'num_channels': int,
                        'host_redshift': float,
                        'frac_lin_pol': float})

        # Clean up columns
        df.repeater_of_objid = df.repeater_of_objid.replace(r'^\s*$', np.nan,
                                                            regex=True)

        # Split columns
        # DM columns
        for c in ('galactic_max_dm', 'reports_dm'):
            cols = df[c].str.partition(' (')[[0, 2]]
            value, model = cols[0], cols[2]

            if c == 'reports_dm':
                c = 'dm'

            df[c] = value.astype(float)
            df[c + '_model'] = model.str.strip(')')
        df.drop(['reports_dm'], axis=1, inplace=True)

        # Coordinate columns
        for c in ('reports_ra', 'reports_decl'):
            cols = df[c].str.partition(' (')[[0, 2]]
            value, err = cols[0], cols[2]
            df[c.split('_')[-1]] = value
            df[c.split('_')[-1] + '_err'] = err.str.strip(')')
            df = df.drop([c], axis=1, errors='ignore')

        cols = df.flux.str.partition(' (')[[0, 2]]
        value, err = cols[0], cols[2]
        df['flux'] = value.astype(float)  # In Jy
        err = err.str.strip(')')
        err[err == ''] = np.nan
        df['flux_err'] = err.astype(float)

        cols = df.tel_inst.str.partition('_')[[0, 2]]
        df['telescope'], df['telescope_mode'] = cols[0], cols[2]
        df.drop(['tel_inst'], axis=1, inplace=True)

        cols = df.fluence.str.strip(' Jy ms').str.partition(' (')[[0, 2]]
        df['fluence'] = cols[0].astype(float)
        err = cols[2].str.strip(')')
        err[err == ''] = np.nan
        df['fluence_err'] = err.astype(float)

        for c in ('burst_width', 'scattering_time'):
            cols = df[c].str.strip(' ms').str.partition(' (')[[0, 2]]
            df[c] = cols[0].astype(float)
            err = cols[2].str.strip(')')
            err[err == ''] = np.nan
            df[c + '_err'] = err.astype(float)

        cols = df.burst_bandwidth.str.strip(' MHz').str.partition(' (')[[0, 2]]
        df['burst_bandwidth'] = cols[0].astype(float)
        err = cols[2].str.strip(')')
        err[err == ''] = np.nan
        df['burst_bandwidth_err'] = err.astype(float)

        # Commenting out as some FAST entries seem to use GHz
        # instead of MHz units in the reference frequency field
        #for c in ('ref_freq', 'inst_bandwidth'):
        #    df[c] = df[c].str.strip(' MHz').astype(float)

        s = 'sampling_time'
        df[s] = df[s].str.strip(' ms').astype(float)

        cols = df.rm.str.strip(' rad/m2').str.partition(' (')[[0, 2]]
        df['rm'] = cols[0].astype(float)
        err = cols[2].str.strip(')')
        err[err == ''] = np.nan
        df['rm_err'] = err.astype(float)

        # Convert time columns to datetime objects
        dates = ('time_received', 'barycentric_event_time', 'discovery_date',
                 'photometry_date', 'lastmodified')
        for c in dates:
            df[c] = pd.to_datetime(df[c], format='%Y-%m-%d %H:%M:%S')

        # Sort the dataframe
        df = df.reindex(sorted(df.columns), axis=1)

        self.df = df

        self.coor_trans()

    def filter(self,
               one_offs=True,
               repeaters=True,
               repeat_bursts=False):
        """Filter frbcat in various ways."""
        if one_offs is False:
            # Only keep repeaters
            self.df = self.df[~self.df.repeater_of_objid.isnull()]

        if repeaters is False:
            # Drops any repeater sources
            self.df = self.df[self.df.repeater_of_objid.isnull()]

        if repeat_bursts is False:
            # Only keeps one detection of repeaters
            self.df = self.df.sort_values('photometry_date',
                                          ascending=True)
            dup = (~self.df.duplicated(subset=['repeater_of_objid'],
                                       keep='first'))
            self.df = self.df[dup | (self.df['repeater_of_objid'].isnull())]

        self.df = self.df.sort_index()

    def coor_trans(self):
        """Apply coordinate transformations."""
        def trans(df):

            # Clean up some errors in frbcat
            if df['decl'].count(':') < 2:
                df['decl'] = df['decl'] + ':00'
            if df['ra'].count(':') < 2:
                df['ra'] = df['ra'] + ':00'

            ra, dec = misc.frac_deg(df['ra'], df['decl'])
            gl, gb = misc.radec_to_lb(ra, dec, frac=True)
            df['ra_frac'] = ra
            df['dec_frac'] = dec
            df['gl_frac'] = gl
            df['gb_frac'] = gb
            return df

        self.df = self.df.apply(trans, axis=1)


if __name__ == '__main__':
    tns = TNS().df
    import IPython; IPython.embed()
