import setuptools
import os

_setup_dir = os.path.dirname(os.path.abspath(__file__))


reqs = [
    'networkx==2.6.3',
    'tqdm==4.62',
]


setuptools.setup(
    name="graph_tools",
    version="0.1.1",
    author="Cheng Shen",
    author_email="shenchg126@gmail.com",
    description="Everything you need to build and analyze transaction data",
    packages=['graph_tools', 'graph_tools.components'],
    include_package_data=True,
    python_requires='>=3.8',
    install_requires=reqs,
    )
