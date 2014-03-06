# Apache DataFu website

We use [Middleman](http://middlemanapp.com/) to generate the website content. This requires Ruby.

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
