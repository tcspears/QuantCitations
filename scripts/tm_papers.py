import pandas as pd
import numpy as np
import spacy
import en_core_web_sm

hjm_data = pd.read_csv("/Users/taylor/hjm_descendents.csv")

nlp = en_core_web_sm.load()

