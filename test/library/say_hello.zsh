# The little brute of a module system: this file rides in the library
# ConfigMap, the layout script untars it to src/library, and a pipeline that
# named library in its sources gets this function for the price of a source
# statement. No autoload, no fpath, no registry — a Job has no keystroke to
# make laziness worth anything.

function say_hello_world {
    print 'hello, world'
}
