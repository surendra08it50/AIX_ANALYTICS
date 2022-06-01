from setuptools import find_packages, setup

VERSION = "0.0.1"
setup(
    name="dashboard_analytics",
    version=VERSION,
    author="Dashboard Analytics Services team",
    url="https://amatbb.amat.com/projects/AITAICSP/repos/repo-name/browse",
    package_dir={"": "src"},
    packages=find_packages('src'),
    install_requires=[
        # 'fastapi',
        # 'docker',
        # 'uvicorn',
        # 'fastapi~=0.68.1',
        # 'sqlalchemy==1.3',
        # 'bcrypt~=3.2.0',
        # 'pyjwt~=2.0.1',
        # 'python-dotenv~=0.19.0',
        # 'setuptools~=52.0.0',
        # 'boto3',
        # 'clickhouse-sqlalchemy',
        # 'sniffio', 
        ]
)

