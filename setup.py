from setuptools import setup

setup(
    name='ElasticSearch Agent',
    version='0.1',
    description='An agent to maintain metadata records in Elastic Search',
    url='',
    author='Mike Metcalfe',
    author_email='mike@webtide.co.za',
    license='MIT',
    packages=['agent'],
    install_requires=[
        'requests',
        'cherrypy',
        'elasticsearch_dsl',
    ],
    python_requires='>=3',
)
