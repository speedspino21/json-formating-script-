from toolz import curried
from toolz.functoolz import pipe
from toolz.dicttoolz import get_in
import json
from toolz.dicttoolz import assoc_in, get_in
from toolz.itertoolz import concat
import pandas as pd

def explode(d, keys):
    values = get_in(keys, d)
    if isinstance(values, list):
        for v in values:
            yield assoc_in(d, keys, v)
    else:
        yield d


def unwind(iterator, keys):
    return concat(map(lambda d: explode(d, keys), iterator))

def flatten_data():

    with open('Survey_Final.json', encoding="utf8") as data_file:
        data = json.load(data_file)


    pages = pipe(data["pages"],
                 curried.map(lambda d: {
                        "part": get_in(["name"], d),
                        "title": get_in(["title"], d),
                    }),
                 list,
                 )

    sections = pipe(data["pages"],
                    lambda i: unwind(i, ["elements"]),
                    curried.map(lambda d: {
                       "part": get_in(["name"], d),
                       "type": get_in(["elements", "type"], d),
                       "section": get_in(["elements", "name"], d),
                   }),
                    list,
                    )

    questions = pipe(data["pages"],
                     lambda i: unwind(i, ["elements"]),
                     lambda i: unwind(i, ["elements", "elements"]),
                     curried.map(lambda d: {
                         "section": get_in(["elements", "name"], d),
                         "question": get_in(["elements", "elements", "name"], d),
                         "type": get_in(["elements", "elements", "type"], d),
                         "title": get_in(["elements", "elements", "title"], d),
                         "readOnly": get_in(["elements", "elements", "readOnly"], d),
                         "description": get_in(["elements", "elements", "description"], d),
                         "isRequired": get_in(["elements", "elements", "isRequired"], d),
                         "colCount": get_in(["elements", "elements", "colCount"], d),
                     }),
                     list,
    )

    dropdown_choices = pipe(data["pages"],
                            lambda i: unwind(i, ["elements"]),
                            lambda i: unwind(i, ["elements", "elements"]),
                            lambda i: unwind(i, ["elements", "elements", "choices"]),
                            curried.filter(lambda d: get_in(["elements", "elements", "choices"], d) is not None),
                            curried.filter(lambda d: get_in(["elements", "elements", "type"], d) == "dropdown" ),
                            curried.map(lambda d: {
                       "Section": get_in(["elements", "name"], d),
                       "type": get_in(["elements", "elements", "type"], d),
                       "question": get_in(["elements", "elements", "name"], d),
                       "choices_value": get_in(["elements", "elements", "choices", "value"], d),
                       "choices_text": get_in(["elements", "elements", "choices", "text"], d),
                   }),
                            list,
                            )

    radiogroup_choices = pipe(data["pages"],
                            lambda i: unwind(i, ["elements"]),
                            lambda i: unwind(i, ["elements", "elements"]),
                            lambda i: unwind(i, ["elements", "elements", "choices"]),
                            curried.filter(lambda d: get_in(["elements", "elements", "choices"], d) is not None),
                            curried.filter(lambda d: get_in(["elements", "elements", "type"], d) == "radiogroup" ),
                            curried.map(lambda d: {
                       "Section": get_in(["elements", "name"], d),
                       "type": get_in(["elements", "elements", "type"], d),
                       "question": get_in(["elements", "elements", "name"], d),
                       "choices_value": get_in(["elements", "elements", "choices", "value"], d),
                       "choices_text": get_in(["elements", "elements", "choices", "text"], d),
                   }),
                            list,
                            )


    matrixdropdown_column = pipe(data["pages"],
                   lambda i: unwind(i, ["elements"]),
                   lambda i: unwind(i, ["elements", "elements"]),
                   lambda i: unwind(i, ["elements", "elements", "columns"]),
                   curried.filter(lambda d: get_in(["elements", "elements", "columns"], d) is not None),
                   curried.filter(lambda d: get_in(["elements", "elements", "type"], d) == "matrixdropdown" ),
                   curried.map(lambda d: {
                       "Section": get_in(["elements", "name"], d),
                       "type": get_in(["elements", "elements", "type"], d),
                       "question": get_in(["elements", "elements", "name"], d),
                       "choices_name": get_in(["elements", "elements", "columns", "name"], d),
                       "choices_title": get_in(["elements", "elements", "columns", "title"], d),
                       "choices_cellType": get_in(["elements", "elements", "columns", "cellType"], d),
                       "choices_isRequired": get_in(["elements", "elements", "columns", "isRequired"], d),
                       "choices_inputType": get_in(["elements", "elements", "columns", "inputType"], d),
                   }),
                   list,
    )

    radiogroup_choices = pipe(data["pages"],
                            lambda i: unwind(i, ["elements"]),
                            lambda i: unwind(i, ["elements", "elements"]),
                            lambda i: unwind(i, ["elements", "elements", "choices"]),
                            curried.filter(lambda d: get_in(["elements", "elements", "choices"], d) is not None),
                            curried.filter(lambda d: get_in(["elements", "elements", "type"], d) == "radiogroup" ),
                            curried.map(lambda d: {
                       "Section": get_in(["elements", "name"], d),
                       "type": get_in(["elements", "elements", "type"], d),
                       "question": get_in(["elements", "elements", "name"], d),
                       "choices_value": get_in(["elements", "elements", "choices", "value"], d),
                       "choices_text": get_in(["elements", "elements", "choices", "text"], d),
                   }),
                            list,
                            )

    matrixdynamic_validators = pipe(data["pages"],
                            lambda i: unwind(i, ["elements"]),
                            lambda i: unwind(i, ["elements", "elements"]),
                            lambda i: unwind(i, ["elements", "elements", "validators"]),
                            curried.filter(lambda d: get_in(["elements", "elements", "validators"], d) is not None),
                            curried.filter(lambda d: get_in(["elements", "elements", "type"], d) == "matrixdynamic" ),
                            curried.map(lambda d: {
                       "Section": get_in(["elements", "name"], d),
                       "type": get_in(["elements", "elements", "type"], d),
                       "question": get_in(["elements", "elements", "name"], d),
                       "validators_type": get_in(["elements", "elements", "validators", "type"], d),
                   }),
                            list,
                            )


    matrixdynamic_column = pipe(data["pages"],
                   lambda i: unwind(i, ["elements"]),
                   lambda i: unwind(i, ["elements", "elements"]),
                   lambda i: unwind(i, ["elements", "elements", "columns"]),
                   curried.filter(lambda d: get_in(["elements", "elements", "columns"], d) is not None),
                   curried.filter(lambda d: get_in(["elements", "elements", "type"], d) == "matrixdynamic" ),
                   curried.map(lambda d: {
                       "Section": get_in(["elements", "name"], d),
                       "type": get_in(["elements", "elements", "type"], d),
                       "question": get_in(["elements", "elements", "name"], d),
                       "columns_name": get_in(["elements", "elements", "columns", "name"], d),
                       "columns_title": get_in(["elements", "elements", "columns", "title"], d),
                       "columns_cellType": get_in(["elements", "elements", "columns", "cellType"], d),
                       "columns_isRequired": get_in(["elements", "elements", "columns", "isRequired"], d),
                       "columns_inputType": get_in(["elements", "elements", "columns", "inputType"], d),
                   }),
                   list,
    )

    matrixdynamic_column_validators = pipe(data["pages"],
                   lambda i: unwind(i, ["elements"]),
                   lambda i: unwind(i, ["elements", "elements"]),
                   lambda i: unwind(i, ["elements", "elements", "columns"]),
                   lambda i: unwind(i, ["elements", "elements", "columns","validators"]),
                   curried.filter(lambda d: get_in(["elements", "elements", "columns"], d) is not None),
                   curried.filter(lambda d: get_in(["elements", "elements", "columns","validators"], d) is not None),
                   curried.filter(lambda d: get_in(["elements", "elements", "type"], d) == "matrixdynamic" ),
                   curried.map(lambda d: {
                       "Section": get_in(["elements", "name"], d),
                       "type": get_in(["elements", "elements", "type"], d),
                       "question": get_in(["elements", "elements", "name"], d),
                       "name": get_in(["elements", "elements", "columns", "name"], d),
                       "title": get_in(["elements", "elements", "columns", "title"], d),
                       "columns_validators_type": get_in(["elements", "elements", "columns", "validators","type"], d),
                       "columns_validators_text": get_in(["elements", "elements", "columns", "validators","text"], d),
                       "columns_validators_minLength": get_in(["elements", "elements", "columns", "validators","minLength"], d),
                       "columns_validators_maxLength": get_in(["elements", "elements", "columns", "validators","maxLength"], d),
                       "columns_validators_allowDigits": get_in(["elements", "elements", "columns", "validators","allowDigits"], d),
                   }),
                   list,
    )

    df_pages = pd.DataFrame.from_records(pages)
    df_sections = pd.DataFrame.from_records(sections)
    df_questions = pd.DataFrame.from_records(questions)
    df_dropdown_choices = pd.DataFrame.from_records(dropdown_choices)
    df_radiogroup_choices = pd.DataFrame.from_records(radiogroup_choices)
    df_matrixdropdown_column = pd.DataFrame.from_records(matrixdropdown_column)
    df_matrixdynamic_validators = pd.DataFrame.from_records(matrixdynamic_validators)
    df_matrixdynamic_column = pd.DataFrame.from_records(matrixdynamic_column)
    df_matrixdynamic_column_validators = pd.DataFrame.from_records(matrixdynamic_column_validators)


    print("-------------pages-----------")
    print(df_pages)
    print("-------------sections-----------")
    print(df_sections)
    print("-------------questions-----------")
    print(df_questions)
    print("-------------dropdown choices-----------")
    print(df_dropdown_choices)
    print("-------------radiogroup choices-----------")
    print(df_radiogroup_choices)
    print("-------------matrixdropdown column-----------")
    print(df_matrixdropdown_column)
    print("-------------matrixdynamic validators-----------")
    print(df_matrixdynamic_validators)
    print("-------------matrixdynamic column-----------")
    print(df_matrixdynamic_column)
    print("-------------matrixdynamic column validators-----------")
    print(df_matrixdynamic_column_validators)



flatten_data()
