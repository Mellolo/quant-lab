"""MyPipeline — 书 Ch9 Snippet 9.2.

sklearn.Pipeline 的 fit 不直接接受 sample_weight；
MyPipeline 在 fit 时把 sample_weight 转发给最后一步。
"""

from __future__ import annotations

from sklearn.pipeline import Pipeline


class MyPipeline(Pipeline):
    """支持 sample_weight 的 sklearn Pipeline.

    用法::

        pipe = MyPipeline([('scaler', StandardScaler()), ('clf', RandomForestClassifier())])
        pipe.fit(X, y, sample_weight=w)
    """

    def fit(self, X, y, sample_weight=None, **fit_params):
        if sample_weight is not None:
            last_step_name = self.steps[-1][0]
            fit_params[f"{last_step_name}__sample_weight"] = sample_weight
        return super().fit(X, y, **fit_params)
