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
        'cherrypy==14.0.0',
        'elasticsearch_dsl',
        'cheroot==6.0.0',
        'tempora==1.10',
        'elasticsearch_dsl==6.1.0',
    ],
    python_requires='>=3',
)
