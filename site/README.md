# Apache DataFu website

We use [Middleman](http://middlemanapp.com/) to generate the website content. This requires Ruby.  It's highly recommended that you use something like [rbenv](https://github.com/sstephenson/rbenv) to manage your Ruby versions.  The website content has been successfully generated using Ruby version `2.2.2`.

## Setup

Install bundler if you don't already have it:

    gem install bundler

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

    svn co https://svn.apache.org/repos/asf/incubator/datafu apache-datafu-website

## Commit the changes

In the `apache-datafu-website` folder, delete the old content, which we will be replacing
with new content:

    cd apache-datafu-website
    rm -rf site

Now copy the built content to the `apache-datafu-website` folder, replacing the old `site` folder:

    cp -r ~/Projects/incubator-datafu/site/build site

Check what has changed:

    svn status

If you have added or removed files, you may need to run `svn add` or `svn rm`.

Once you are satisfied with the changes you can commit.
