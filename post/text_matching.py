
import re

def get_matching_text_data(text, query, start, end):
    text = text.lower()
    query = query.lower()
    pattern = re.compile(query, re.IGNORECASE)
    matches = []

    def find_matches(subtext):
        # List to hold the start positions of each match
        starts = [0]

        while starts:
            start = starts.pop()
            for match in pattern.finditer(subtext[start:]):
                match_text = match.group()
                matches.append(match_text)
                # Calculate the new start position within the matched text
                new_start = start + match.start() + 1
                if new_start < len(subtext):
                    starts.append(new_start)
    find_matches(text)
    return matches
def new_get_match(text, query, start, end):

    pattern = re.compile(query, re.IGNORECASE)
    matches = []

    for match in pattern.finditer(text):
        if match.group():
            match_text = match.group()
            # matches.append(match_text)
            last_occurrence = match_text.rfind(start)
            last_match = match_text[last_occurrence:]
            matches.append(last_match)

    return matches
def get_matching(text, query, start, end):
    text = text.lower()
    query = query.lower()
    start = start.lower()
    end = end.lower()
    match_value=None
    list_match=[]
    matches=new_get_match(text, query, start, end)
    if len(matches)>0:
        numbet_of_sapce = 3
        if ' ' in query:
            numbet_of_sapce=4

        for match in matches:
            list_match=match.split(' ')

            if len(list_match)<=numbet_of_sapce and (start in match and end in match):
                if len(match)<40:
                    match_value=match
                    return match_value
    return match_value

