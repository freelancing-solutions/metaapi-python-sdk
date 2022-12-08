import setuptools

with open("README.rst", "r") as fh:
    long_description = fh.read()

install_requires = [
   'aiohttp==3.7.4', 'python-engineio==3.14.2', 'typing-extensions~=3.10.0.0', 'iso8601', 'pytz',
   'python-socketio[asyncio_client]==4.6.0', 'requests==2.24.0', 'websockets==9.1', 'httpx==0.23.0',
   'metaapi-cloud-copyfactory-sdk>=6.0.0', 'metaapi-cloud-metastats-sdk>=3.1.2',
   'metaapi-cloud-risk-management-sdk>=1.2.1'
]

tests_require = [
    'pytest==6.2.5', 'pytest-mock==3.8.2', 'pytest-asyncio==0.16.0', 'asynctest==0.13.0', 'mock==4.0.3',
    'freezegun==1.0.0', 'respx==0.19.2'
]

setuptools.setup(
    name="metaapi_cloud_sdk",
    version="20.8.2",
    author="Agilium Labs LLC",
    author_email="agiliumtrade@agiliumtrade.ai",
    description="SDK for MetaApi, a professional cloud forex API which includes MetaTrader REST API "
                "and MetaTrader websocket API. Supports both MetaTrader 5 (MT5) and MetaTrader 4 (MT4). CopyFactory"
                "copy trading API included. (https://metaapi.cloud)",
    long_description=long_description,
    long_description_content_type="text/x-rst",
    keywords=['metaapi.cloud', 'MetaTrader', 'MetaTrader 5', 'MetaTrader 4', 'MetaTrader5', 'MetaTrader4', 'MT', 'MT4',
              'MT5', 'forex', 'trading', 'API', 'REST', 'websocket', 'client', 'sdk', 'cloud', 'free', 'copy trading',
              'copytrade', 'copy trade', 'trade copying'],
    url="https://github.com/agiliumtrade-ai/metaapi-python-sdk",
    include_package_data=True,
    package_dir={'metaapi_cloud_sdk': 'lib'},
    packages=['metaapi_cloud_sdk'],
    install_requires=install_requires,
    tests_require=tests_require,
    license='SEE LICENSE IN LICENSE',
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: Other/Proprietary License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)