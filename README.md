[![Tests](https://github.com/sagargg/ckanext-datastore-uploader/workflows/Tests/badge.svg?branch=main)](https://github.com/sagargg/ckanext-datastore-uploader/actions)
 

When there is a high volume of requests on the datastore dump URL, it creates a heavy load on both the CKAN and Postgres database servers. In order to alleviate this load, this extension generates a blob file uploads it to filestore, and then serves downloads directly from filestore.

## How this work 

It runs separate job each time when the datastore table is updated via the datastore create/update action. While the blob upload is in progress, the data is served via a generic datastore dump URL. Once the upload is completed, the data is served directly from the filestore.
It also works with s3filestorage. 


## Installation

To install ckanext-datastore-uploader:

1. Activate your CKAN virtual environment, for example:

    . /usr/lib/ckan/default/bin/activate

2. Clone the source and install it on the virtualenv

        git clone https://github.com/sagargg/ckanext-datastore-uploader.git
        cd ckanext-datastore-uploader
        pip install -e .
	    pip install -r requirements.txt

3. Add `datastore-uploader` to the `ckan.plugins` setting in your CKAN
   config file (by default the config file is located at
    `/etc/ckan/default/ckan.ini`).

4. Restart CKAN. For example if you've deployed CKAN with Apache on Ubuntu:

        sudo service apache2 reload

5. Run ckan background job worker 

        paster --plugin=ckan jobs worker --config=/ckan.ini # ckan >= 2.9
        ckan -c  ckan.ini jobs worker  # ckan < 2.9




## Config settings

If you are using [ckanext-noanonaccess](https://github.com/datopian/ckanext-noanonaccess) extension then allow this blueprint

    ckanext.noanonaccess.allowed_blueprint = datastore_uploader.dump


## Developer installation

To install ckanext-datastore-uploader for development, activate your CKAN virtualenv and
do:

    git clone https://github.com/sagargg/ckanext-datastore-uploader.git
    cd ckanext-datastore-uploader
    python setup.py develop
    pip install -r dev-requirements.txt


## License

[AGPL](https://www.gnu.org/licenses/agpl-3.0.en.html)
