# CSTBox OBIX gateway extension

This repository contains the code for the extension adding a gateway
between the [CSTBox framework](http://cstbox.cstb.fr) internal message bus 
and [OBIX](http://www.obix.org/) based systems.

It works by polling periodically the OBIX gateway for present values of a 
set of OBIX sensors and emits the corresponding sensor events on the CSTBox
message bus, applying the common rules related to value changes and 
notifications time to live.

The translation between OBIX sensors and CSTBox variables is driven by
a JSON configuration file, including a mapping table along with the
OBIX gateway connection settings.