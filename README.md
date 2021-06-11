# Query the Fast Radio Burst Catalogue in Python
<a href="https://ascl.net/2011.011"><img src="https://img.shields.io/badge/ascl-2011.011-blue.svg?colorB=262255" alt="ascl:2011.011" /></a>

## Installation

```sh
pip3 install frbcat
```

## Usage
Get a Pandas DataFrame of `frbcat` using
```python
from frbcat import Frbcat
df = Frbcat().df
```
Or get the chime repeaters using
```python
from frbcat import ChimeRepeaters
df = ChimeRepeaters().df
```
Or get the FRBs from the Transient Name Server, which as of Sep 2020 should have all FRBs
```python
from frbcat import TNS
tns = TNS(tns_name='my_user_name', tns_id='my_user_id')
df = tns.df
units = tns.units
```

## Requirements

* pandas
* numpy
* requests

## Contributing

Contributions are what make the open source community such an amazing place to be learn, inspire, and create. Any contributions you make are appreciated.

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request


## License

Distributed under the MIT License. See `LICENSE` for more information.
