import codecs
import os

from .base import describe, to_html
import json


class ProfileGenerate(object):
    def profilegen(df, bins=10, sample=5, corr_reject=0.9, config={}, **kwargs):
        sample = df.limit(sample).toPandas()

        description_set = describe(df, bins=bins, corr_reject=corr_reject, config=config, **kwargs)

        return description_set, sample
