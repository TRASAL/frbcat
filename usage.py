"""How to query frbcat."""
from frbcat import Frbcat

cat = Frbcat(oneoffs=True,  # Include oneoffs
             repeaters=True,  # Include repeaters
             repeat_bursts=True,  # Multiple bursts per repeater
             update='monthly',  # How often to update local copy
             path=None,  # Where to save a copy, defaults to Downloads
             mute=True  # Whether to print progress in the terminal
             )

# Get pandas DataFrame
df = cat.df

# Show the paper in which the parameters are defined
cat.parameters()
