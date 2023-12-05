# IVIP-demo data

This repository contains the data (video recordings and transcriptions) included in the IVIP-demo corpus. The data has been anonymised such that neither the videos nor the transcriptions contain any personal data.

The transcriptions are available in the [CHAT transcription format](https://talkbank.org/manuals/CHAT.html) (`original/raw`) obtained by transcribing videos with the CLAN program (https://talkbank.org/manuals/CLAN.pdf).

The transcriptions are also available as XML files (`original/xml`) (a conversion from CHAT to XML has been done by Språkbanken Text). These XML files were then processed with the [Sparv Pipeline](https://spraakbanken.gu.se/sparv) in order to generate linguistic annotations. The resulting data including the annotations can be downloaded here: https://spraakbanken.gu.se/resurser/ivip-demo

The file `preprocess.py` holds the code for converting the CHAT transcriptions into Sparv-friendly XML. Run `python preprocess.py -h` to get information on how to use the script.

Please not that you need to use [Git Large File Storage ](https://git-lfs.com/) in order to clone the media files.
