from setuptools import setup, find_packages

with open('README.md') as readme_file:
    README = readme_file.read()

with open('HISTORY.md') as history_file:
    HISTORY = history_file.read()

setup_args = dict(
    name='logging',
    version='1.0.0',
    description='Logging module',
    long_description_content_type="text/markdown",
    long_description=README + '\n\n' + HISTORY,
    license='MIT',
    packages=find_packages(),
    author='dialog',

)

install_requires = [
    'snowflake.connector',
    'boto3'
]

if __name__ == '__main__':
    setup(**setup_args, install_requires=install_requires)
