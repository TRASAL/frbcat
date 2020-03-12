"""Miscellaneous helpful functions."""
import inspect
import numpy as np


def pprint(*s, output=True):
    """Hack to make for more informative print statements."""
    f = inspect.stack()[1][1].split('/')[-1]
    m = '{:13.13} |'.format(f)

    if output:
        print(m, *s)
    else:
        lines = []
        for e in s:
            line = [str(m) + ' ' + str(f) for f in e.split('\n')]
            lines.append('\n'.join(line))
        return '\n'.join(lines)


def frac_deg(ra, dec):
    """Convert coordinates expressed in hh:mm:ss to fractional degrees."""
    # Inspired by Joe Filippazzo calculator
    rh, rm, rs = [float(r) for r in ra.split(':')]
    ra = rh*15 + rm/4 + rs/240
    dd, dm, ds = [float(d) for d in dec.split(':')]
    if dd < 0:
        sign = -1
    else:
        sign = 1
    dec = dd + sign*dm/60 + sign*ds/3600
    return ra, dec


def radec_to_lb(ra, dec, frac=False):
    """
    Convert from ra, dec to galactic coordinates.

    Formulas from 'An Introduction to Modern Astrophysics (2nd Edition)' by
    Bradley W. Carroll, Dale A. Ostlie (Eq. 24.16 onwards).

    NOTE: This function is not as accurate as the astropy conversion, nor as
    the Javascript calculators found online. However, as using astropy was
    prohibitively slow while running over large populations, we use this
    function. While this function is not as accurate, the under/over
    estimations of the coordinates are equally distributed meaning the errors
    cancel each other in the limit of large populations.

    Args:
        ra (string): Right ascension given in the form '19:06:53'
        dec (string): Declination given in the form '-40:37:14'
        frac (bool): Denote whether coordinates are already fractional or not
    Returns:
        gl, gb (float): Galactic longitude and latitude [fractional degrees]

    """
    if not frac:
        ra, dec = frac_deg(ra, dec)

    a = np.radians(ra)
    d = np.radians(dec)

    # Coordinates of the galactic north pole (J2000)
    a_ngp = np.radians(12.9406333 * 15.)
    d_ngp = np.radians(27.1282500)
    l_ngp = np.radians(123.9320000)

    sd_ngp = np.sin(d_ngp)
    cd_ngp = np.cos(d_ngp)
    sd = np.sin(d)
    cd = np.cos(d)

    # Calculate galactic longitude
    y = cd*np.sin(a - a_ngp)
    x = cd_ngp*sd - sd_ngp*cd*np.cos(a - a_ngp)
    gl = - np.arctan2(y, x) + l_ngp
    gl = np.degrees(gl) % 360

    # Shift so in range -180 to 180
    if isinstance(gl, np.ndarray):
        gl[gl > 180] = -(360 - gl[gl > 180])
    else:
        if gl > 180:
            gl = -(360 - gl)

    # Calculate galactic latitude
    gb = np.arcsin(sd_ngp*sd + cd_ngp*cd*np.cos(a - a_ngp))
    gb = np.degrees(gb) % 360

    if isinstance(gb, np.ndarray):
        gb[gb > 270] = -(360 - gb[gb > 270])
    else:
        if gb > 270:
            gb = -(360 - gb)

    return gl, gb
