#!/bin/bash

function clean_jupyter(){
  rm -r .ipynb_checkpoints
}

function clean_docs(){
  rm -r docs/build
}

function clean_coverage(){
  rm -r coverage
  rm .coverage coverage.xml
}

function clean_packaging(){
  rm -r build dist google_pandas_load.egg-info
}

function clean(){
  clean_jupyter
  clean_docs
  clean_coverage
  clean_packaging
}
