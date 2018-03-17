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

    svn co https://svn.apache.org/repos/asf/datafu apache-datafu-website

## Commit the changes

In the `apache-datafu-website` folder, delete the old content, which we will be replacing
with new content:

    cd apache-datafu-website
    rm -rf site

Now copy the built content to the `apache-datafu-website` folder, replacing the old `site` folder:

    cp -r ~/Projects/datafu/site/build site

This procedure unfortunately removes the javadocs too, which we want to keep and are not stored in the git repo.  We should add a script to make this easier.  In the meantime you can revert the deleted javadoc files with something resembling the commands below.  Check the `revert_list.txt` file before proceeding.

    svn status | grep "\!" | cut -c 9- | grep -E "(datafu|hourglass)/\d\.\d\.\d/" > revert_list.txt
    svn revert --targets revert_list.txt

If this is a new release, make sure you have built the javadocs first.  If you are in the release branch for the repo, you can run `gradle assemble -Prelease=true` to generate the javadocs.  The `release=true` flag ensure SNAPSHOT does not appear in the name.  If you are building from the source release this isn't necessary and `gradle assemble` is fine.  Copy the new javadocs from the release into the site.

    cp -r ~/Projects/datafu/datafu-pig/build/docs/javadoc site/docs/datafu/x.y.z
    cp -r ~/Projects/datafu/datafu-hourglass/build/docs/javadoc site/docs/hourglass/x.y.z
    svn add site/docs/datafu/x.y.z
    svn add site/docs/hourglass/x.y.z

Open the new javadocs and confirm they are correct before checking them in.  For example, make sure that the version is correct and the version does not have SNAPSHOT in the name.

Check what has changed:

    svn status

If you have added or removed files, you may need to run `svn add` or `svn rm`.  If there are a lot of changes you may want to run `svn status | grep ?` to make sure you didn't miss anything.

Once you are satisfied with the changes you can commit.
