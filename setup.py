from distutils.core import setup

setup(
    name='pandas-showcase',
    version='0.1',
    packages=[],
    url='',
    license='',
    author='Iryna Cherniavska',
    author_email='i [dot] chernyavska [at] gmail [dot] com',
    description='Test some ways of loading fixed-width data.',
    requires=[
        'psutil==2.2.1',
        'memory_profiler==0.32',
        'gocept.recordserialize==0.2',
        'matplotlib==1.4.3',
        'psycopg2==2.6',
        'SQLAlchemy==1.0.2',
        'openpyxl==1.8.6',
        'pandas==0.16.0',
        'FixedWidth==0.99',
        'django-copybook==1.0.3'
    ]
)
