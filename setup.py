import setuptools

with open('requirements.txt') as fobj:
    REQUIREMENTS = [l.strip() for l in fobj.readlines()]

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pysixdesk",
    version="0.0.1",
    author='Xiaohan Lu,A. Mereghetti',
    author_email='luxh@ihep.ac.cn,Alessio.Mereghetti@cern.ch',
    description="A python interface to manage and control the workflow of SixTrack jobs",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/SixTrack/pysixdesk",
    packages=setuptools.find_packages(),
    install_requires=REQUIREMENTS,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
