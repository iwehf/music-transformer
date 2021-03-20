"""
Copyright 2020 Aditya Gomatam.

This file is part of music-transformer (https://github.com/spectraldoy/music-transformer), my project to build and
train a Music Transformer. music-transformer is open-source software licensed under the terms of the GNU General
Public License v3.0. music-transformer is free software: you can redistribute it and/or modify it under the terms of
the GNU General Public License as published by the Free Software Foundation, either version 3 of the License,
or (at your option) any later version. music-transformer is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
See the GNU General Public License for more details. A copy of this license can be found within the GitHub repository
for music-transformer, or at https://www.gnu.org/licenses/gpl-3.0.html.
"""

import torch
import mido
import time
import argparse
from masking import *
from tokenizer import *
from vocabulary import *

"""
Functionality to use Music-Transformer model after (or during) training to generate audio
"""


def greedy_decode(model, inp, mode="categorical", temperature=1.0, k=None):
    """
    TODO
    Args:
        model:
        inp:
        mode:
        temperature: functions for spicier decode sampling
        k:

    Returns:

    """
    # convert input tokens to list of token ids
    inp = events_to_indices(inp)

    # make sure inp starts with the start token
    if inp[0] != start_token:
        inp = [start_token] + inp

    # convert to torch tensor and convert to correct dimensions for masking
    inp = torch.tensor(inp, dtype=torch.int64, device=device)
    inp = inp.unsqueeze(0)
    n = inp.dim() + 2

    # parameters for decode sampling
    if not callable(temperature):
        temperature__ = temperature
        del temperature

        def temperature(x):
            return temperature__

    if k is not None and not callable(k):
        k__ = k
        del k

        def k(x):
            return k__

    # autoregressively generate output
    try:
        with torch.no_grad():
            while True:
                # get next predicted idx
                predictions = model(inp, mask=create_mask(inp, n))
                # divide logits by temperature as a function of current length of sequence
                predictions /= temperature(inp[-1].shape[-1])

                # sample the next predicted idx
                if mode == "argmax":
                    prediction = torch.argmax(predictions[..., -1, :], dim=-1)

                elif k is not None:
                    # get top k predictions, where k is a function of current length of sequence
                    top_k_preds = torch.topk(predictions[..., -1, :], k(inp[-1].shape[-1]), dim=-1)
                    # sample top k predictions
                    predicted_idx = torch.distributions.Categorical(logits=top_k_preds.values[..., -1, :]).sample()
                    # get the predicted id
                    prediction = top_k_preds.indices[..., predicted_idx]

                elif mode == "categorical":
                    prediction = torch.distributions.Categorical(logits=predictions[..., -1, :]).sample()

                else:
                    raise ValueError("Invalid mode or top k passed in")

                # if we reached the end token, immediately output
                if prediction == end_token:
                    return inp.squeeze()

                # else cat and move to the next prediction
                inp = torch.cat(
                    [inp, prediction.view(1, 1)],
                    dim=-1
                )

    except (KeyboardInterrupt, RuntimeError):
        # generation takes a long time, interrupt in between to save whatever has been generated until now
        # RuntimeError is in case the model generates more tokens that there are absolute positional encodings for
        pass

    # extra batch dimension needs to be gotten rid of, so squeeze
    return inp.squeeze()


def audiate(token_ids, save_path="gneurshk.mid", tempo=512820, verbose=False):
    """
    TODO: desc and sample rate / font / gain

    Args:
        token_ids:
        save_path:
        tempo:
        verbose:

    Returns:

    """
    # set file to a midi file
    if save_path.endswith(".midi"):
        save_path = save_path[:-1]
    elif save_path.endswith(".mid"):
        pass
    else:
        save_path += ".mid"

    # create and save the midi file
    print(f"Saving midi file at {save_path}...") if verbose else None
    mid = list_parser(token_ids, fname=save_path[:-4], tempo=tempo)
    mid.save(save_path)

    """ save other file formats
    if save_flac:
        flac_path = save_path[:-4] + ".flac"
        print(f"Saving flac file at {flac_path}...") if verbose else None
        fs = FluidSynth()
        fs.midi_to_audio(save_path, flac_path)

    if save_wav:
        wav_path = save_path[:-4] + ".wav"
        print(f"Saving wav file at {wav_path}...") if verbose else None
        fs = FluidSynth()
        fs.midi_to_audio(save_path, wav_path)

        # useful for ipynbs
        return Audio(wav_path)
    """

    return


def generate(model_, inp, save_path="./bloop.mid", temperature=1.0, mode="categorical", k=None,
             tempo=512820, verbose=False):
    """
    TODO
    NOTE: long time

    Args:
        model_:
        inp:
        save_path:
        temperature:
        mode:
        k:
        tempo:
        verbose:

    Returns:

    """
    # greedy decode
    print("Greedy decoding...") if verbose else None
    start = time.time()
    token_ids = greedy_decode(model=model_, inp=inp, mode=mode, temperature=temperature, k=k)
    end = time.time()
    print(f"Generated {len(token_ids)} tokens.", end=" ") if verbose else None
    print(f"Time taken: {round(end - start, 2)} secs.") if verbose else None

    # generate audio
    return audiate(token_ids=token_ids, save_path=save_path, tempo=tempo, verbose=verbose)


if __name__ == "__main__":
    from model import MusicTransformer
    from hparams import hparams

    def check_positive(x):
        if x is None:
            return x
        x = int(x)
        if x <= 0:
            raise argparse.ArgumentTypeError(f"{x} is not a positive integer")
        return x

    def load_model(filepath):
        """
        Load a MusicTransformer from a saved pytorch state_dict and hparams. The input filepath should point to a .pt
        file in which has been saved a dictionary containing the model state dict and hparams, ex:
        torch.save(filepath, {
            "state_dict": MusicTransformer.state_dict(),
            "hparams": hparams (dict)
        })

        Args:
            filepath (str): path to single .pt file containing the dictionary as described above

        Returns:
            the loaded MusicTransformer model
        """
        file = torch.load(filepath)
        if "hparams" not in file:
            file["hparams"] = hparams

        model = MusicTransformer(**file["hparams"]).to(device)
        model.load_state_dict(file["state_dict"])
        model.eval()
        return model

    parser = argparse.ArgumentParser(
        prog="generate.py",
        description="Generate midi audio with a Music Transformer!"
    )
    parser.add_argument("path_to_model", help="string path to a .pt file at which has been saved a dictionary "
                                              "containing the model state dict and hyperparameters", type=str)
    parser.add_argument("save_path", help="path at which to save the generated midi file", type=str)
    parser.add_argument("-m", "--mode", help="specify 'categorical' or 'argmax' mode of decode sampling", type=str)
    parser.add_argument("-k", "--top-k", help="top k samples to consider while decode sampling; default: all",
                        type=check_positive)
    parser.add_argument("-t", "--temperature",
                        help="temperature for decode sampling; lower temperature, more sure the sampling, "
                             "higher temperature, more diverse the output; default: 1.0 (categorical sample of true "
                             "model output)",
                        type=float)
    parser.add_argument("-tm", "--tempo", help="approximate tempo of generated sample in BMP", type=check_positive)
    """
    parser.add_argument("-w", "--save-wav", help="flag to save a wav file along with the generated midi file",
                        action="store_true")
    parser.add_argument("-f", "--save-flac", help="flag to save a flac file along with the generated midi file",
                        action="store_true")
    """
    parser.add_argument("-v", "--verbose", help="verbose output flag", action="store_true")

    args = parser.parse_args()

    # fix arguments
    temperature_ = float(args.temperature) if args.temperature else 1.0
    mode_ = args.mode if args.mode else "categorical"
    k_ = int(args.top_k) if args.top_k else None
    tempo_ = int(60 * 1e6 / int(args.tempo)) if args.tempo else 600000

    music_transformer = load_model(args.path_to_model)
    generate(model_=music_transformer, inp=["<start>"], save_path=args.save_path,
             temperature=temperature_, mode=mode_, k=k_, tempo=tempo_, verbose=args.verbose)
