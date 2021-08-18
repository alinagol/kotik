from math import pi
from typing import Any, Dict

import matplotlib
import matplotlib.pyplot as plt

matplotlib.use("agg")


def emotions_chart(media: Dict[str, Any], upload_folder: str) -> str:
    emotions = ["sadness", "anger", "joy", "disgust", "fear"]
    dimensions = len(emotions)

    values = [media[emotion] for emotion in emotions]
    values.append(values[0])

    angles = [n / float(dimensions) * 2 * pi for n in range(dimensions)]
    angles += angles[:1]

    fig = plt.figure(figsize=(3, 3))
    ax = fig.add_subplot(111, polar=True)
    plt.xticks(angles[:-1], emotions, color="grey", size=12)
    ax.set_rlabel_position(0)
    plt.yticks(
        [0.25, 0.5, 0.75], ["0.25", "0.5", "0.75"], color="grey", size=8
    )
    plt.ylim(0, 1)
    ax.plot(angles, values, linewidth=1, linestyle="solid")
    ax.fill(angles, values, "b", alpha=0.1)

    filename = f'{upload_folder}/{media["slug"]}.png'
    plt.tight_layout()
    plt.savefig(filename)
    plt.clf()

    return filename
