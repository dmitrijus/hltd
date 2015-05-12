cd $1
echo installing elasticsearch plugin $3 ...
bin/plugin -s --url file:///opt/fff/esplugins/$2 --install $3

