# Apache DataFu website

We use [Middleman](http://middlemanapp.com/) to generate the website content. This requires Ruby.  It's highly recommended that you use something like [rbenv](https://github.com/sstephenson/rbenv) to manage your Ruby versions.  The website content has been successfully generated using Ruby version `2.2.2`.

```
rbenv install 2.2.2
rbenv local 2.2.2
```

## Setup

Install bundler if you don't already have it:

    gem install bundler -v 1.16.2

Install gems required by website (includes middleman):

    bundle install

## Run the Server

Middleman includes a server that can be run locally.  When making changes to the website
it is usually good practice to run the server to see what the changes look like in a
browser.

    bundle exec middleman

Now visit [http://localhost:4567/](http://localhost:4567/) to see the website in action.

## Build the website

The static content can be built with:

    bundle exec middleman build

This will produces the content in the `/build` directory.

## Check out the website source

The static website content is located in another repo:

    svn co https://svn.apache.org/repos/asf/datafu apache-datafu-website

## Build and commit docs

To generate the docs, run the commands below.  If you are in a release branch or building from a source release then the version should not include SNAPSHOT in the name.  Confirm this is the case by opening the docs after they are built.

    ./gradlew clean
    ./gradlew :datafu-spark:scaladoc :datafu-pig:javadoc :datafu-hourglass:javadoc

Assuming current version is `x.y.z`, copy the docs to the site repo into new version directories:

    cp -r ~/Projects/datafu/datafu-pig/build/docs/javadoc ~/Projects/apache-datafu-website/site/docs/datafu/x.y.z
    cp -r ~/Projects/datafu/datafu-hourglass/build/docs/javadoc ~/Projects/apache-datafu-website/site/docs/hourglass/x.y.z
    cp -r ~/Projects/datafu/datafu-spark/build/docs/scaladoc ~/Projects/apache-datafu-website/site/docs/spark/x.y.z

Add and commit the changes.

## Commit the website changes

In the `apache-datafu-website` folder, delete the old content, which we will be replacing
with new content:

    cd apache-datafu-website
    rm -rf site

Now copy the built content to the `apache-datafu-website` folder, replacing the old `site` folder:

    cp -r ~/Projects/datafu/site/build site

This procedure unfortunately removes the javadocs too, which we want to keep and are not stored in the git repo.  We should add a script to make this easier.  In the meantime you can revert the deleted javadoc files with something resembling the commands below.  Check the `revert_list.txt` file before proceeding.

    svn status | grep "\!" | cut -c 9- | grep -E "(datafu|hourglass|spark)/\d\.\d\.\d/" > revert_list.txt
    svn revert --targets revert_list.txt

Check what has changed:

    svn status

If you have added or removed files, you may need to run `svn add` or `svn rm`.  If there are a lot of changes you may want to run `svn status | grep ?` to make sure you didn't miss anything.

Once you are satisfied with the changes you can commit.
