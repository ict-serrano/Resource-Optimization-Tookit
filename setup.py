from distutils.core import setup

setup(
    name='SERRANO - ROT',
    version='2.0',
    description='SERRANO - Resource Optimization Toolkit',
    author='Aristotelis Kretsis',
    author_email='akretsis@mail.ntua.gr',
    url='',
    packages=[
        'serrano_rot',
        'serrano_rot.api',
        'serrano_rot.algorithms',
        'serrano_rot.algorithms.Dummy',
        'serrano_rot.controller',
        'serrano_rot.engine',
        'serrano_rot.utils'
    ]
)
