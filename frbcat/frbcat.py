"""Do things with frbcat."""
import datetime
import glob
import io
import os
import pandas as pd
import requests
import numpy as np

from .misc import pprint, frac_deg, radec_to_lb


class Frbcat():
    """Query FRBCAT by using Frbcat().df ."""

    def __init__(self,
                 oneoffs=True,
                 repeaters=True,
                 repeat_bursts=True,
                 update='monthly',
                 path=None,
                 one_per_frb=True):
        """Query FRBCAT.

        Args:
            oneoffs (type): Whether to include oneoffs. Defaults to True.
            repeaters (type): Whether to include repeaters. Defaults to True.
            repeat_bursts (type): Whether to include multiple bursts per
                repeater. Defaults to True.
            update (str): Always get a new version (True), get a new version
                once a month ('monthly'), or never get a new version (False).
                Defaults to 'monthly'.
            path (type): Directory in which to save the csv file. Defaults to
                your Downloads folder.
            one_per_frb (type): Frbcat can have multiple determined values per
                burst (using different methods etc). Stick to the default of
                True unless you feel brave.

        Returns:
            type: Description of returned object.

        """
        if path is None:
            path = os.path.expanduser('~') + '/Downloads/'

        self.path = path
        self.update = update
        self.repeaters = repeaters
        self.repeat_bursts = repeat_bursts
        self.oneoffs = oneoffs

        # Get frbcat data
        self.get(update=update)

        self.clean()
        self.coor_trans()

        self.filter(one_per_frb=one_per_frb,
                    repeat_bursts=repeat_bursts,
                    repeaters=repeaters,
                    one_offs=oneoffs)

        # Just to neaten up
        self.df = self.df.sort_values('utc', ascending=False)
        self.df = self.df.reindex(sorted(self.df.columns), axis=1)

        self.pandas = self.df

    def url_to_df(self, url):
        """Convert a url of a JSON table to a Pandas DataFrame.

        Args:
            url (str): URL to the webpage

        Returns:
            DataFrame: DataFrame of JSON table

        """
        try:
            s = requests.get(url).content
            f = io.StringIO(s.decode('utf-8'))

            series = []
            for entry in pd.read_json(f)['products']:
                series.append(pd.Series(entry))
            df = pd.concat(series, axis=1).T

            return df

        except ValueError:
            pass

    def urls_to_df(self, endings, url):
        """
        Use Series to loop over multiple webpages.

        Proceed to concatenate them to a single DataFrame

        Args:
            endings (iterables): The list/series/column over which to loop
            url (str): The base url

        Returns:
            DataFrame

        """
        dfs = []
        for ending in endings:
            full_url = f'{url}{ending}'
            df = self.url_to_df(full_url)
            if isinstance(df, pd.DataFrame):
                dfs.append(df)

        if dfs:
            return pd.concat(dfs, ignore_index=True)
        else:
            return None

    def get(self, update=True, save=True):
        """
        Get frbcat from online or from a local file.

        Args:
            update (bool): Whether to get a new version of frbcat, or use a
            local version

        """
        # Check whether a copy of FRBCAT has already been downloaded
        # Ensures frbcat is only queried once a month
        path = self.path + '/frbcat_'
        path += str(datetime.datetime.today()).split()[0][:-3]
        path += '-??.csv'
        exists = glob.glob(path)
        if update == 'monthly' and exists:
            update = False

        if update:
            try:
                pprint('Attempting to retrieve FRBCAT from www.frbcat.org')

                # First get all FRB names from the main page
                pprint(' - Getting FRB names')
                url = 'http://frbcat.org/products/'
                main_df = self.url_to_df(url)

                # Then get any subsequent analyses (multiple entries per FRB)
                pprint(' - Getting subsequent analyses')
                url = 'http://frbcat.org/product/'
                frb_df = self.urls_to_df(main_df.frb_name, url)

                # Find all frb note properties
                pprint(' - Getting notes on FRBs')
                url = 'http://frbcat.org/frbnotes/'
                frbnotes_df = self.urls_to_df(set(frb_df.index), url)
                if frbnotes_df is not None:
                    frbnotes_df = frbnotes_df.add_prefix('frb_notes_')

                # Find all notes on radio observation parameters
                pprint(' - Getting radio observation parameters')
                url = 'http://frbcat.org/ropnotes/'
                ropnotes_df = self.urls_to_df(set(frb_df.index), url)
                if ropnotes_df is not None:
                    ropnotes_df = ropnotes_df.add_prefix('rop_notes_')

                # Find all radio measurement parameters
                pprint(' - Getting radio measurement parameters')
                url = 'http://frbcat.org/rmppubs/'
                rmppubs_df = self.urls_to_df(set(frb_df.index), url)
                rmppubs_df = rmppubs_df.add_prefix('rmp_pub_')

                # Have skipped
                # 'http://frbcat.org/rmpimages/<rmp_id>' (images)
                # 'http://frbcat.org/rmpnotes/<rmp_id>' (empty)

                # Merge all databases together
                try:
                    df = pd.merge(frb_df,
                                  frbnotes_df,
                                  left_on='frb_id',
                                  right_on='frb_notes_frb_id',
                                  how='left')
                except TypeError:
                    df = frb_df

                df = pd.merge(df,
                              ropnotes_df,
                              left_on='rop_id',
                              right_on='rop_notes_rop_id',
                              how='left')

                self.df = pd.merge(df,
                                   rmppubs_df,
                                   left_on='rmp_id',
                                   right_on='rmp_pub_rmp_id',
                                   how='left')

                pprint('Succeeded')

                if save:
                    date = str(datetime.datetime.today()).split()[0]
                    path = f'{self.path}/frbcat_{date}.csv'
                    self.df.to_csv(path)

                update = False

            # Unless there's no internet
            except requests.ConnectionError:
                update = False

        if update is False:
            # Find latest version of frbcat
            f = max(glob.glob(self.path + '/frbcat*.csv'),
                    key=os.path.getctime)
            pprint(f"Using {f.split('/')[-1]}")
            self.df = pd.read_csv(f)

    def clean(self):
        """Clean up the data."""
        # Lower all column names
        self.df.columns = map(str.lower, self.df.columns)

        # Convert None's to Nan's
        self.df.fillna(value=np.nan, inplace=True)

        # Clean up column names
        self.df.columns = self.df.columns.str.replace('rop_', '')
        self.df.columns = self.df.columns.str.replace('rmp_', '')

        # There's a problem with mulitple 'id' columns
        cols = [c for c in self.df.columns if not c.endswith('id')]
        self.df = self.df[cols]

        # Split out errors on values
        for c in self.df.columns:
            if self.df[c].dtype == object:
                if any(self.df[c].str.contains('&plusmn', na=False)):
                    val, err = self.df[c].str.split('&plusmn', 1).str
                    self.df[c] = pd.to_numeric(val)
                    self.df[c+'_err'] = pd.to_numeric(err)

        # Split out asymetric errors on values
        for c in self.df.columns:
            if self.df[c].dtype == object:
                if any(self.df[c].str.contains('<sup>', na=False)):
                    upper = "<span className='supsub'><sup>"
                    val, rest = self.df[c].str.split(upper, 1).str
                    upper, rest = rest.str.split('</sup><sub>', 1).str
                    lower, _ = rest.str.split('</sub></span>', 1).str
                    self.df[c] = pd.to_numeric(val)
                    self.df[c+'_err_up'] = pd.to_numeric(upper)
                    self.df[c+'_err_down'] = pd.to_numeric(lower)

        # Conversion table
        convert = {'mw_dm_limit': 'dm_mw',
                   'width': 'w_eff',
                   'flux': 's_peak',
                   'redshift_host': 'z',
                   'spectral_index': 'si',
                   'dispersion_smearing': 't_dm',
                   'dm_error': 'dm_err',
                   'scattering_timescale': 't_scat',
                   'sampling_time': 't_samp'}

        self.df.rename(columns=convert, inplace=True)

        # Ensure columns are the right datatype
        self.df.w_eff = pd.to_numeric(self.df.w_eff, errors='coerce')

        # Add some extra columns
        self.df['fluence'] = self.df['s_peak'] * self.df['w_eff']

        # Gives somewhat of an idea of the pulse width upon arrival at Earth
        self.df['w_arr'] = (self.df['w_eff']**2 -
                            self.df['t_dm']**2 -
                            self.df['t_scat']**2 -
                            self.df['t_samp']**2)**0.5

        # Reduce confusion in telescope names
        small_tele = self.df['telescope'].str.lower()
        self.df['telescope'] = small_tele

        # Set utc as dates
        self.df['utc'] = pd.to_datetime(self.df['utc'])

        # Replace chime/frb with chime
        if any(self.df['telescope'].str.contains('chime/frb', na=False)):
            val, _ = self.df['telescope'].str.split('/', 1).str
            self.df['telescope'] = val

        # Remove any enters in titles
        self.df.pub_description = self.df.pub_description.str.replace('\n', '')

        # Split population into repeaters etc
        pd.options.mode.chained_assignment = None
        self.df['type'] = np.where(self.df.duplicated('frb_name'), 'repeater',
                                   'one-off')

    def filter(self,
               one_offs=True,
               repeaters=True,
               repeat_bursts=False,
               one_per_frb=True):
        """Filter frbcat in various ways."""
        if one_per_frb is True:
            # Only keep rows with the largest number of parameters
            # so that only one row per detected FRB remains
            self.df['count'] = self.df.count(axis=1)
            self.df = self.df.sort_values('count', ascending=False)
            self.df = self.df.drop_duplicates(subset=['utc'])

        if one_offs is False:
            # Only keep repeaters
            self.df = self.df[self.df.duplicated(['frb_name'])]

        if repeaters is False:
            # Drops any repeater sources
            self.df = self.df.drop_duplicates(subset=['frb_name'], keep=False)

        if repeat_bursts is False:
            # Only keeps one detection of repeaters
            self.df = self.df.sort_values('utc')
            self.df = self.df.drop_duplicates(subset=['frb_name'], keep='first')

        self.df = self.df.sort_index()

    def coor_trans(self):
        """Apply coordinate transformations."""
        def trans(df):

            # Clean up some errors in frbcat
            if df['decj'].count(':') < 2:
                df['decj'] = df['decj'] + ':00'
            if df['raj'].count(':') < 2:
                df['raj'] = df['raj'] + ':00'

            ra, dec = frac_deg(df['raj'], df['decj'])
            gl, gb = radec_to_lb(ra, dec, frac=True)
            df['ra'] = ra
            df['dec'] = dec
            df['gl'] = gl
            df['gb'] = gb
            return df

        self.df = self.df.apply(trans, axis=1)


if __name__ == '__main__':
    f = Frbcat()
