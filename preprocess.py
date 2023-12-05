"""
IVIP preprocessing script

Requires lxml (https://lxml.de/)


Create filepaths.txt:
ssh fkkorp@k2.spraakdata.gu.se find /var/www/html_sb/korp_data/ivip/Testkorpus -type f -not -name "*.cex" -not -name "*.cha" -not -name "*.DS_Store" > filepaths.txt

Fetch transription files from server:
rsync -trv --include '*.cex' --exclude '*.*' fkkorp@k2.spraakdata.gu.se:/export/res/ivip/Testkorpus original/rawdata
"""

import argparse
import datetime
import re
import sys
from pathlib import Path

from lxml import etree

try:
    from filepaths import paths as mediapaths
except ImportError:
    pass

################################################################################
# Command line args
################################################################################

parser = argparse.ArgumentParser()

subparsers = parser.add_subparsers(dest="command", title="commands", metavar="<command>")
subparsers.required = True

filepaths_parser = subparsers.add_parser("filepaths",
    help="Create a file containing a mapping between transcriptions and media files")
filepaths_parser.add_argument("--infile", "-i", default="filepaths.txt",
                               help="Input file containing list of paths to all media files")
filepaths_parser.add_argument("--outfile", "-o", default="filepaths.py", help="Output filename")

preprocess_parser = subparsers.add_parser("xml",
    help="Preprocess all transcription files transforming them into XML")
preprocess_parser.add_argument("--overwrite", "-f", action="store_true", help="Overwrite existing XML files")
preprocess_parser.add_argument("--indir", "-i", default="original/rawdata",
                               help="Root input for input CHAT transcription files")
preprocess_parser.add_argument("--outdir", "-o", default="original/xml", help="Root dir for output XML files")

################################################################################
# Constants and metadata
################################################################################

# Cities and institution second code:
CITIES = {"Åbo": ("service",
                  {"AST": ("Åbo Svenska Teater", "Kassa, teater"),
                   "ASA": ("Åbo Akademis bibliotek", "Info, bibliotek"),
                   "LUC": ("Luckan", "Kassa, kulturcentrum")}),
          "Helsingfors": ("service",
                          {"LUC": ("Luckan", "Kassa, kulturcentrum"),
                           "HAN": ("Svenska handelshögskolans bibliotek", "Info, bibliotek"),
                           "SVE": ("Svenska teatern", "Kassa, teater")}),
          "Jakobstad": ("service",
                        {"CAM": ("Lipputoimisto", "Kassa, evenemang")}),
          "Karleby": ("service",
                      {"LUC": ("Luckan", "Kassa, kulturcentrum")}),
          "Vasa": ("service",
                   {"WAS": ("Wasa teater", "Kassa, teater"),
                    "STU": ("Studio Ticket", "Kassa, evenemang")}),
          "Göteborg": ("service",
                       {"LB": ("Lorensbergsteatern", "Kassa, teater"),
                        "ST": ("Stadsteatern 1", "Kassa, teater"),
                        "STA": ("Stadsteatern 2", "Kassa, teater"),
                        "GOT": ("GotEvent", "Kassa, evenemang")}),
          "Karlstad": ("service",
                       {"SCA": ("Scala", "Kassa, evenemang"),
                        "KAR": ("Scala", "Kassa, evenemang")}),
          "Luleå": ("service",
                    {"NOR2": ("Norrbottens Museum", "Kassa, kulturcentrum")}),
          "Osby": ("service",
                   {"BIB": ("Osby bibliotek", "Info, bibliotek")}),
          "Stockholm": ("service",
                        {"BIB": ("Stockholms universitetsbibliotek", "Info, bibliotek"),
                         "DRA": ("Dramaten", "Kassa, teater")},
                        ),
          "Umeå": ("service",
                   {"BIL": ("Biljettcentrum", "Kassa, kulturcentrum")}),
          "Raseborg": ("service",
                       {"TEA": ("Raseborgs sommarteater", "Kassa, teater")}),
          }

# Third code:
DATATYPES = {"RL": "filmad inspelning",
             "TEL": "telefoninspelning"
             }

# # Special characters:
# TAGS = {
#     # "\x15": "[timestamp_marker]",
#     "\u2191": "shift to high pitch",
#     "\u2193": "shift to low pitch",
#     "\u21D7": "rising to high",
#     "\u2197": "rising to mid",
#     "\u2192": "level",
#     "\u2198": "falling to mid",
#     "\u21D8": "falling to low",
#     "\u221E": "unmarked ending",
#     "\u2248": "no break continuation",
#     "=": "technical continuation",
#     "\u2261": "uptake (internal)",
#     ".": "inhalation",
#     "\u223E": "constriction",
#     "\u21BB": "pitch reset",
#     "*": "laugh in a word",
#     "\u201E": "tag or final particle",
#     "\u2021": "vocative or summons",
#     "\u0323": "Arabic dot",
#     "\u02B0": "Arabic aspiration",
#     "\u0304": "stressed syllable",
#     "\u0294": "glottal stop",
#     "\u0295": "Hebrew glottal",
#     "\u030C": "caron",
#     ":": "drawl",
#     "#": "creaky",
#     "§": "unsure",
#     "¤": "louder",
#     "[": "begin overlap",
#     "]": "end overlap",
# }

# # Paired special characters:
# TAG_PAIRS = {"\u2206": ("faster", "&gt;&lt;"),
#              "\u2207": ("slower", "&lt;&gt;"),
#              "\u204E": ("creaky", "#"),
#              "\u2047": ("unsure", "§"),
#              "\u00B0": ("softer",
#              "\u25C9": ("louder", "¤"),
#              "\u2581": ("low pitch",
#              "\u2594": ("high pitch",
#              "\u263A": ("smile voice",
#              "\u222C": ("whisper",
#              "\u03AB": ("yawn",
#              "\u222E": ("singing ",
#              "\u00A7": ("precise"
#              }

# OVERLAP = {"\u2308": ("top begin overlap", "["),
#            "\u2309": ("top end overlap", "]"),
#            "\u230A": ("bottom begin overlap", "["),
#            "\u230B": ("bottom end overlap", "]")
#            }

# Specail symbols to be converted into symbols that are easier to type
TRANSLATE = {"\u224B": ("technical continuation", "="),
             "\u2219": ("inhalation", "."),
             "\u1F29": ("laugh in a word", "*"),
             "\u204E": ("creaky", "#"),
             "\u2047": ("unsure", "§"),
             "\u25C9": ("louder", "¤"),
             }

################################################################################
# Main functions
################################################################################

def preprocess(rawdir, xmldir, overwrite=False):
    """Convert all files to xml."""
    try:
        mediapaths
    except NameError:
        print("Error: File filepaths.py is not available. Please create it by running the 'filepaths' command!")
        exit()

    for dirpath in Path(rawdir).glob("**/*"):
        if dirpath.is_file():
            # # For debugging one particular file
            # if dirpath.name != "ABO_AST_RL_004.indnt.cex":
            #     continue
            filename = replace_parenteses(dirpath.name[:dirpath.name.find(".")] + ".xml")
            try:
                fname_meta = extract_filename_meta(filename, dirpath)
            except Exception:
                print(f"Error with file '{dirpath.name}'")
                raise
            newdir = make_new_dir(xmldir, dirpath)
            xmlfile = newdir / filename

            # Check if file exists
            if not xmlfile.is_file() or overwrite:
                print(f"Processing file '{dirpath.name}'")

                # Collect contents and metadata from file
                meta = []
                contents = []
                with open(dirpath) as chatfile:
                    processing_meta = True
                    for line in chatfile:
                        if line.startswith("@"):
                            meta.append(line)
                        elif line.startswith("*"):
                            processing_meta = False
                            contents.append(line)
                        else:
                            if processing_meta:
                                meta[-1] = meta[-1].rstrip() + " " + line.lstrip()
                            else:
                                contents.append(line)

                file_metadata = extract_file_meta(meta)
                textelem = etree.Element("text")
                fill_metadata(textelem, fname_meta, file_metadata)
                extract_utterances(contents, textelem, file_metadata)
                treestring = etree.tostring(textelem, encoding="UTF-8").decode("UTF-8")
                new_treestring = process_content(treestring)

                with open(xmlfile, "w") as f:
                    # print(f"Writing {xmlfile}")
                    f.write(new_treestring)


def get_filepaths(infile, outfile):
    """
    Create a file containing a mapping between transcription files
    and media files.

    Copy video and sound files to protected Korp directory:
    ssh fkkorp@k2.spraakdata.gu.se rsync -trv --exclude "*.cex" --exclude "*.DS_Store" /export/res/ivip/Testkorpus /export/htdocs_sb/korp_data/ivip/data

    To create infile, run:
    ssh fkkorp@k2.spraakdata.gu.se find /export/htdocs_sb/korp_data/ivip/data/Testkorpus -type f -not -name "*.cex" -not -name "*.DS_Store" > filepaths.txt
    """

    with open(outfile, "w") as outf:
        outf.write("paths = {\n")

        with open(infile, "r") as f:
            for line in f:
                line = line.strip()
                path = line[32:]
                name = path.split("/")[-1]
                name = name[:name.find(".")].rstrip("censurerad")
                line = f'    "{replace_parenteses(name)}": "{path}",\n'
                outf.write(line)

        outf.write("}\n")
    print(f"Done writing '{outfile}'")

################################################################################
# Auxiliaries
################################################################################

def make_new_dir(xmldir, dirpath):
    """Calculate and create directory structure."""
    country = dirpath.parts[3]
    newdir = Path(xmldir) / country
    if not newdir.is_dir():
        newdir.mkdir(parents=True)
    return newdir


def extract_utterances(lines, textelem, file_metadata):
    """Divide transcriptions into utterances and define their type."""
    participants = file_metadata[0]
    code = ""
    last_timeinfo = False

    # for chunk in chunks:
    for line in lines:
        line = line.rstrip()

        # Catch line continuations
        if not (line.startswith("*") or line.startswith("%com")):
            line = code + "\t" + line.lstrip()

        # Drop pauses
        if line.startswith("*PPP"):
            utterance = etree.SubElement(textelem, "utterance")
            utterance.set("speaker_id", "paus")
            content = line.split("\t")[1]
            speaker = None
            content, timeinfo = fix_utterance(utterance, content, speaker, participants, last_timeinfo)

        # Comments
        elif line.startswith("%com"):
            code = line.split("\t")[0]
            utterance = etree.SubElement(textelem, "utterance")
            utterance.set("speaker_id", "kommentar")
            content = line.split("\t")[1]
            speaker = None
            content, timeinfo = fix_utterance(utterance, content, speaker, participants, last_timeinfo)

        # "Normal" utterances
        elif line.startswith("*"):
            code = line.split("\t")[0]
            speaker = line.split("\t")[0].strip("*:\n")
            content = line.split("\t")
            if len(content) > 1:
                content = line.split("\t")[1]
                utterance = etree.SubElement(textelem, "utterance")
                content, timeinfo = fix_utterance(utterance, content, speaker, participants, last_timeinfo)
            else:
                continue

        else:
            print("\nERROR\n")
            print(line.encode("UTF-8"))
            exit()

        # Update timeinfo
        if timeinfo:
            last_timeinfo = timeinfo

        if not content.strip():
            textelem.remove(utterance)
        else:
            # Always insert line break for new utterance
            utterance.tail = "\n"

            # Insert whitespaces before utterance
            whitespaces_groups = re.match(r"^(\s+)(.+)", content)
            if whitespaces_groups:
                whitespaces = whitespaces_groups.group(1)
                content = whitespaces_groups.group(2)
                prev_utterance = utterance.getprevious()
                if prev_utterance.tag == "utterance":
                    prev_utterance.tail = "\n" + whitespaces

            utterance.text = content


def fix_utterance(utterance, content, speaker, participants, last_timeinfo):
    """Fill utterance with meta data."""
    # Fix time stamp
    timeinfo = last_timeinfo
    if "\x15" in content:
        timestamp = content[content.find("\x15") + 1:content.rfind("\x15")]
        timeinfo = timestamp.split("_")
        content = content[:content.find("\x15")].rstrip()
        utterance.set("start", timeinfo[0])
        utterance.set("end", timeinfo[1])
    # No timestamp, take previous stamp (if available)
    elif last_timeinfo:
        utterance.set("start", last_timeinfo[0])
        utterance.set("end", last_timeinfo[1])

    # Fill speaker meta data
    if speaker:
        utterance.set("speaker_id", speaker)
        utterance.set("speaker_role", participants[speaker][0])
        if participants[speaker][2]:
            utterance.set("speaker_age", participants[speaker][2])
        if participants[speaker][3]:
            utterance.set("speaker_gender", participants[speaker][3])
        if participants[speaker][4]:
            utterance.set("speaker_region", participants[speaker][4])

    return content, timeinfo


def fill_metadata(textelem, fname_meta, file_metadata):
    """Add general metadata to textelem."""
    country, city, _inst, mediatype, rectype, consent_id, fileloc, place = fname_meta
    participants, _mediafile, date = file_metadata

    textelem.set("country", country)
    textelem.set("city", city)
    textelem.set("place", place)
    textelem.set("mediatype", mediatype)
    textelem.set("type", rectype)
    textelem.set("consentid", consent_id)

    # Point out media file
    mfilename = fileloc[fileloc.rfind("/") + 1:fileloc.rfind(".")]
    mfilepath = fileloc[:fileloc.rfind("/") + 1]
    mfileext = fileloc.split("/")[-1].split(".")[-1]
    textelem.set("mediafilepath", mfilepath)
    textelem.set("mediafile", mfilename)
    textelem.set("mediafileext", mfileext)

    textelem.set("participants", ", ".join([k + " " + v[0] for (k, v) in participants.items() if k != "PPP"]))

    fixed_date = validate_date(date)
    textelem.set("date", fixed_date)


def validate_date(date):
    """Validate date format and print warnings."""
    try:
        datetime.datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        if date == "2014-18-11":
            print("  Date changed from 2014-18-11 to 2014-11-18")
            return "2014-11-18"
        elif date == "2013-13-11":
            print("  Date changed from 2013-13-11 to 2013-11-13")
            return "2013-11-13"
        elif not date:
            print("  WARNING: Missing date information!")
            return ""
        else:
            print(f"  WARNING: Incorrect date: {date}")
            return ""
    return date


def extract_file_meta(lines):
    """Extract meta data from file header."""
    id_chunks = []
    for line in lines:
        if line.startswith("@Participants:"):
            participants_chunk = line[14:].strip()
        elif line.startswith("@ID:"):
            id_chunks.append(line[4:].strip())
        elif line.startswith("@Media:"):
            media = line[7:].strip()
        elif line.startswith("@Location:"):
            location = line[10:].strip()

    # Parse @Participants
    participants_chunk = participants_chunk.split(", ")
    participants = {}
    for i in participants_chunk:
        code = i.split()[0]
        if len(i.split()) > 2:
            participants[code] = [i.split()[1], i.split()[2]]
        else:
            participants[code] = [i.split()[1], ""]

    # Parse @ID
    # Participant: [role1, role2, age, gender, region]
    for i in id_chunks:
        x = i.split("|")
        participants[x[2]].append(x[3][:x[3].find(";")])
        participants[x[2]].append(x[4])
        participants[x[2]].append(x[5])

    # Parse @Media
    mediafile = media.split(", ")[0]
    # Parse @Location
    if location.split()[-1].startswith("20"):
        date = location.split()[-1]
    else:
        date = ""

    return [participants, mediafile, date]


def extract_filename_meta(f, dirpath):
    """Collect meta data from file name and directory structure."""
    parts = dirpath.parts[3:]
    country = parts[0]
    city = parts[1]
    if city == "Abo":
        city = "Åbo"
    if city == "Goteborg":
        city = "Göteborg"

    fname = f[:f.find(".")].rstrip("_censurerad")
    fname = fname.strip()
    codes = fname.split("_")

    rectype = CITIES[city][0]
    inst = CITIES[city][1][codes[1]][0]
    place = CITIES[city][1][codes[1]][1]
    datatype = DATATYPES[codes[2]]
    consent_id = fname
    fileloc = mediapaths.get(fname, "")  # Get file location from filepaths.py
    if not fileloc:
        print(f"WARNING: no video file found for file '{f}' (key: '{fname}')")
    return [country, city, inst, datatype, rectype, consent_id, fileloc, place]


def process_content(treestr):
    """Process the actual transcription contents and do some normalisation of extralinguistic tokens."""
    newtree = []
    for n, line in enumerate(treestr.split("\n")):

        # Skip processing of first and last line
        if n > 0 and line != "</text>":
            # Remove utterance XML
            head = line[:line.find(">") + 1]
            tail = line[line.rfind("<"):]
            line = line[line.find(">") + 1:line.rfind("<")]

            # Fix seperate inhalation
            line = re.sub(r"\u2219h", r" .h ", line)

            # Slower and faster tags
            line = re.sub(r"\u2206(.+)\u2206", r"&gt;\1&lt;", line)
            line = re.sub(r"\u2207(.+)\u2207", r"&lt;\1&gt;", line)

            processed_words = []
            words = re.split(r"(\s+)", line)
            for word in words:
                w = etree.Element("w")

                # Shortened forms, overlaps, pauses
                clean_word = re.sub(r"[⌈⌉⌊⌋/]*", "", word)
                if word != clean_word:
                    if "/" in word:
                        w.set("normalised", clean_word)
                        w.set("full", word)
                        shortened_word = re.sub(r"(.+)/([a-zA-ZäöåÄÖÅéÉàÀüÜ]+)(.*)", r"\1\3", word)
                        w.text = shortened_word
                        if any(char in word for char in "⌈⌉⌊⌋"):
                            w.set("type", "|förkortat|överlapp|")
                        else:
                            w.set("type", "|förkortat|")
                    elif any(char in word for char in "⌈⌉⌊⌋"):
                        w.set("type", "|överlapp|")
                        w.set("normalised", clean_word)
                        w.text = word
                        # Pause with overlap
                        if re.search(r"(\(\d*\.\d*\))", word):
                            w.set("type", "|överlapp|paus|")
                    if w.text:
                        word = etree.tostring(w, encoding="UTF-8").decode("UTF-8")

                # Remaining pauses (without overlaps)
                elif re.search(r"(\(\d*\.\d*\))", word):
                    w.set("type", "|paus|")
                    w.text = word
                    word = etree.tostring(w, encoding="UTF-8").decode("UTF-8")

                processed_words.append(word)

            line = "".join(processed_words)

            # # Replace overlap markers
            # for char in OVERLAP.keys():
            #     line = re.sub(char, OVERLAP[char][1], line)

            # Replace CLAN-symbols from translation dict
            for char in TRANSLATE.keys():
                line = re.sub(char, TRANSLATE[char][1], line)

            # Remove double spaces
            line = re.sub(r"\s\s", " ", line)

            # Add utterance xml
            line = head + line + tail

        newtree.append(line)
    newtree = "\n".join(newtree)
    return newtree


def replace_parenteses(filename):
    return re.sub(r"\((.+)\)", r"-\1-", filename)


################################################################################
# Process command line args
################################################################################
if __name__ == "__main__":
    # Parse command line args, print help if none are given
    args = parser.parse_args(args=None if sys.argv[1:] else ["--help"])

    if args.command == "filepaths":
        get_filepaths(args.infile, args.outfile)
    if args.command == "xml":
        preprocess(args.indir, args.outdir, overwrite=args.overwrite)
