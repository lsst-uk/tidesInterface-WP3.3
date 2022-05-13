# Check ZTF Objects from Lasair

## Intro

The software presented in this deliverable is intended to evaluate transient light curves to check whether they meet a specific set of criteria. Work Package 3.3 focuses on the spectroscopic classification of transient events from LSST using the 4MOST/TiDES facility. The majority of extra-galactic transients discovered by LSST being supernovae (SNe), and with Type Ia SNe playing the critical role in our cosmology analysis. 4MOST/TiDES will survey the entire southern sky, collecting spectra of transients and their hosts at an industrial scale, unrivalled in volume by an contemporary facility. TiDES will not, however, dictate the overall strategy or any individual pointing of 4MOST during its operations, nor will TiDES know precisely where 4MOST will observe on any given night. This means TiDES will need to be ready with a target list to accommodate 4MOST observations where ever and whenever they occur. It is, therefore, paramount that TiDES have the software capabilities to rapidly identify targets from the LSST real-time stream. Given that LSST is still under-construction, we are using the Zwicky Transient Facility (ZTF) as our development survey with the intent to scale-up operations in the coming years. In this document we present a small piece of software that can evaluate a ZTF light curve and indicate to the user if the target passes or fails a custom light curve-based selection requirement requirement.

### The code

The entirety of this code is written in the `Python` language, v3.8 was used, but all versions of Python 3 should be compatible. Standard inbuilt Python libraries are needed, with additional libraries including: `numpy`, `pandas`,`json`,`YAML`,`docopts` and `matplotlib`. These required libraries are listed in the `requirements.txt` file. The Lasair Python library (`lasair`) is used to interface with the Lasair broker to retrieve ZTF light curves. This library is still in the beta phase of development -- v0.0.3 at the time of writing. We expect evolution in the package and will maintain compatibility to the official and stable release. Furthermore, the Lasair API is currently only returning complete data for objects observed after 2020, this somewhat limits our ability to test the code against the full ZTF history, but it is adequate to demonstrate the deliverable. The Lasair Python package can be installed following the instructions here: [https://pypi.org/project/lasair/](https://pypi.org/project/lasair/)


### API Tokens
Accessing Lasair services through either the Python library or API requires an authorisation token. For the purposes of this deliverable, the access token can be read from a YAML file or directly used as a command line argument. An example of the contents of a YAML access file is shown below, however this script only requires the `token` keyword.  For security reasons, files containing tokens should be kept away from main development or execution areas. The Lasair API token must be separately obtained from your username and password following the instruction here: [https://lasair-iris.roe.ac.uk/api](https://lasair-iris.roe.ac.uk/api)

```
lasair:
    username: LHamilton44
    password: 7xWDC0820
    token: Cb6442e76373g2M7bwjd738ae1c946d54b8f7993
```

## Example execution

```bash
python checkObjects.py -k 7a635469bb489bd4b70f1bc7bf3f39cffaemmm2a -s /data/chris/tides/tidesInterface-WP3.3/tidesTargeting/tidesSelectionFunctions.yml -n tidesSNZTFSelect -i /data/chris/tides/tidesInterface-WP3.3/tidesTargeting/ztfIAListDemo.dat -o /data/chris/dev_output/Demo/ -p T -c 50
```

### Docs
```
Check the ZTF Objects using Lasair
Usage:
    checkObjects.py [--input=<PATH>] [--output=<PATH>] [--selection=<PATH>] [--name=<NAME>] [--key=<KEY>] [--chunk=<NUM>] [--plot=<BOOL>]
    checkObjects.py -h | --help | --version

Options:
    -i PATH, --input=PATH      The full path to the list containing the ZTF objects.
    -o PATH, --output=PATH     The full path to the output directory.
    -s PATH, --selection=PATH  The full path to the YAML file containing the selection function criteria.
    -n NAME, --name=NAME       The name of the selection function in the YAML file.
    -k KEY, --key=KEY          Your Lasair access token key or path to the YAML file containing it.
    -c NUM, --chunk=NUM        The number of objects to query at once. The Lasair API has a max of 50 objects per call. [default: 50]
    -p BOOL, --plot=BOOL       Do you want to save light curve plots in in the output directory? [default: True]
    -h --help                  Show this screen
    --version                  Show version
```