from desipipe import Queue, Environment, TaskManager, FileManager


queue = Queue('y1_data')
environ = Environment('nersc-cosmodesi')

tm = TaskManager(queue=queue, environ=environ)

tm_corr = tm.clone(scheduler=dict(max_workers=10), provider=dict(provider='nersc', mpiprocs_per_worker=1))
tm_profile = tm.clone(scheduler=dict(max_workers=10), provider=dict(provider='nersc', mpiprocs_per_worker=6, nodes_per_worker=0.2))
tm_sample = tm.clone(scheduler=dict(max_workers=10), provider=dict(provider='nersc', mpiprocs_per_worker=32, nodes_per_worker=0.5))


@tm_corr.python_app
def compute_correlation(data, randoms, output):
    import numpy as np
    from pycorr import TwoPointCorrelationFunction
    from cosmoprimo.fiducial import DESI
    zrange = data.options['zrange']
    data = data.read()
    cosmo = DESI()
    mask = (data['Z'] >= zrange[0]) & (data['Z'] < zrange[1])
    data_positions = [data['RA'][mask], data['DEC'][mask], cosmo.comoving_radial_distance(data['Z'][mask])]
    data_weights = (data['WEIGHT'] * data['WEIGHT_FKP'])[mask]
    edges = (np.linspace(0., 200, 201), np.linspace(-1., 1., 201))
    corr = 0
    for iran in range(2):
        randoms = randoms.get(iran=iran).read()
        mask = (randoms['Z'] >= zrange[0]) & (randoms['Z'] < zrange[1])
        randoms_positions = [randoms['RA'][mask], randoms['DEC'][mask], cosmo.comoving_radial_distance(randoms['Z'][mask])]
        randoms_weights = (randoms['WEIGHT'] * randoms['WEIGHT_FKP'])[mask]
        corr += TwoPointCorrelationFunction(mode='smu', edges=edges, data_positions1=data_positions, randoms_positions1=randoms_positions,
                                            data_weights1=data_weights, randoms_weights1=randoms_weights,
                                            position_type='rdd', los='midpoint', nthreads=64)
    corr.D1D2.attrs['z'] = (data['Z'][mask] * data_weights).cmean() / data_weights.cmean()
    corr.D1D2.attrs['tracer'] = tracer
    output.write(corr)
    return output


def get_template(template_name='standard', z=0.8, klim=None):

    import numpy as np

    """A simple wrapper that returns the template of interest."""

    from desilike.theories.galaxy_clustering import (StandardPowerSpectrumTemplate, ShapeFitPowerSpectrumTemplate,
                                                     WiggleSplitPowerSpectrumTemplate,BandVelocityPowerSpectrumTemplate, DirectPowerSpectrumTemplate, BAOPowerSpectrumTemplate)

    if 'standard' in template_name:
        template = StandardPowerSpectrumTemplate(z=z)
    elif 'shapefit' in template_name:
        template = ShapeFitPowerSpectrumTemplate(z=z)
    elif 'wigglesplit' in template_name:
        template = WiggleSplitPowerSpectrumTemplate(z=z)
    elif 'ptt' in template_name:
        template = BandVelocityPowerSpectrumTemplate(z=z, kp=np.arange(*klim))
    elif 'direct' in template_name:
        template = DirectPowerSpectrumTemplate(z=z)
        template.params['omega_b'].update(fixed=False, prior={'dist': 'norm', 'loc': 0.02237, 'scale': 0.00037})
        template.params['n_s'].update(fixed=True)
    elif 'bao' in template_name:
        template = BAOPowerSpectrumTemplate(z=z)
    return template


def get_theory(theory_name='velocileptors', observable_name='power', b1E=1.9, template=None, ells=(0, 2, 4)):

    """A simple wrapper that returns the theory of interest."""

    from desilike.theories.galaxy_clustering import (LPTVelocileptorsTracerPowerSpectrumMultipoles, LPTVelocileptorsTracerCorrelationFunctionMultipoles,
                                                     LPTMomentsVelocileptorsTracerPowerSpectrumMultipoles, LPTMomentsVelocileptorsTracerCorrelationFunctionMultipoles,
                                                     EPTMomentsVelocileptorsTracerPowerSpectrumMultipoles, EPTMomentsVelocileptorsTracerCorrelationFunctionMultipoles,
                                                     PyBirdTracerPowerSpectrumMultipoles, PyBirdTracerCorrelationFunctionMultipoles,
                                                     DampedBAOWigglesTracerPowerSpectrumMultipoles, DampedBAOWigglesTracerCorrelationFunctionMultipoles)

    kwargs = {}
    euler = False
    if 'bird' in theory_name:
        euler = True
        kwargs.update(eft_basis='westcoast')
        Theory = PyBirdTracerPowerSpectrumMultipoles if observable_name == 'power' else PyBirdTracerCorrelationFunctionMultipoles
    elif 'velo' in theory_name:
        Theory = LPTVelocileptorsTracerPowerSpectrumMultipoles if observable_name == 'power' else LPTVelocileptorsTracerCorrelationFunctionMultipoles
    elif 'lptm' in theory_name:
        Theory = LPTMomentsVelocileptorsTracerPowerSpectrumMultipoles if observable_name == 'power' else LPTMomentsVelocileptorsTracerCorrelationFunctionMultipoles
    elif 'eptm' in theory_name:
        euler = True
        Theory = EPTMomentsVelocileptorsTracerPowerSpectrumMultipoles if observable_name == 'power' else EPTMomentsVelocileptorsTracerCorrelationFunctionMultipoles
    elif 'dampedbao' in theory_name:
        euler = True
        Theory = DampedBAOWigglesTracerPowerSpectrumMultipoles if observable_name == 'power' else DampedBAOWigglesTracerCorrelationFunctionMultipoles

    theory = Theory(template=template, **kwargs)
    # Changes to theory.init.params will remain whatever pipeline is built
    b1 = float(euler) + b1E - 1.
    theory.init.params['b1'].update(value=b1, ref={'limits': [b1 - 0.1, b1 + 0.1]})
    for param in theory.init.params.select(basename=['alpha6']): param.update(fixed=True)
    if 4 not in ells:
        for param in theory.init.params.select(basename=['alpha4', 'sn4*', 'al4_*']): param.update(fixed=True)
    if observable_name != 'power':
        for param in theory.init.params.select(basename=['ce1', 'sn0', 'al0_0']): param.update(fixed=True)
    return theory


def get_fit_setup(tracer, ells=None, theory_name='velocileptors'):
    if ells is None:
        ells = (0, 2, 4)
    if 'bao' in theory_name: ells = (0, 2)
    if tracer.startswith('LRG'):
        z = 0.8
        b0 = 1.7
        smin, kmax = 30., 0.2
        if 'bao' in theory_name: smin, kmax = 40., 0.3
        klim = {ell: [0.02, kmax, 0.005] for ell in ells}
        slim = {ell: [smin, 150., 4.] for ell in ells}
    if tracer.startswith('ELG'):
        z = 1.1
        b0 = 0.84
        smin, kmax = 25., 0.2
        if 'bao' in theory_name: smin, kmax = 40., 0.3
        klim = {ell: [0.02, kmax, 0.005] for ell in ells}
        slim = {ell: [smin, 150., 4.] for ell in ells}
    if tracer.startswith('QSO'):
        z = 1.4
        b0 = 1.2
        smin, kmax = 20., 0.25
        if 'bao' in theory_name: smin, kmax = 40., 0.3
        klim = {ell: [0.02, kmax, 0.005] for ell in ells}
        slim = {ell: [smin, 150., 4.] for ell in ells}
    return z, b0, klim, slim


def get_observable_likelihood(data=None, covariance=None, wmatrix=None, theory_name='velocileptors', ells=(0, 2, 4), template_name='shapefit', observable_name='corr', tracer='LRG',
                              solve=True, save_emulator=False, emulator_fn=None):

    """Return the power spectrum likelihood, optionally computing the emulator (if ``save_emulator``)."""

    import numpy as np
    from desilike.observables.galaxy_clustering import TracerPowerSpectrumMultipolesObservable, TracerCorrelationFunctionMultipolesObservable
    from desilike.likelihoods import ObservablesGaussianLikelihood
    from desilike import utils

    tracer = data.D1D2.attrs['tracer']
    z, b0, klim, slim = get_fit_setup(tracer, ells=ells, theory_name=theory_name)
    z = data.D1D2.attrs['z']

    from cosmoprimo.fiducial import DESI
    fiducial = DESI()
    b1E = b0 / fiducial.growth_factor(z)

    # Load theory
    theory = get_theory(theory_name=theory_name, observable_name=observable_name, template=None, b1E=b1E, ells=klim.keys())
    if 'bao' in theory_name:
        if save_emulator:
            raise ValueError('No need to build an emulator for the BAO model!')
        emulator_fn = None

    if save_emulator or emulator_fn is None:  # No emulator available (yet)
        template = get_template(template_name=template_name, z=z, klim=(klim[0][0], klim[0][1] + 1e-5, klim[0][2]))
        theory.init.update(template=template)
    else:  # Load emulator
        from desilike.emulators import EmulatedCalculator
        calculator = EmulatedCalculator.load(emulator_fn)
        theory.init.update(pt=calculator)

    if observable_name == 'power':
        if utils.is_path(covariance):
            covariance = np.loadtxt(covariance)
            step = 0.005
            covariance = cut_matrix(covariance, np.arange(0. + step / 2., 0.4 + step / 3., step), (0, 2, 4), slim)
        observable = TracerPowerSpectrumMultipolesObservable(klim=klim, data=data, covariance=covariance, wmatrix=wmatrix, kinlim=(0., 0.25), theory=theory)

    if observable_name == 'corr':
        if utils.is_path(covariance):
            covariance = np.loadtxt(covariance)
            step = 4.
            covariance = cut_matrix(covariance, np.arange(20 + step / 2., 200 + step / 3., step), (0, 2, 4), slim)
        observable = TracerCorrelationFunctionMultipolesObservable(slim=slim, data=data, covariance=covariance, theory=theory)

    likelihood = ObservablesGaussianLikelihood(observables=[observable], scale_covariance=1. / 25.)  # likelihood is a callable that returns the log-posterior

    if save_emulator:  # Compute and save emulator
        likelihood()  # to set up k-ranges for the emulator
        from desilike.emulators import Emulator, TaylorEmulatorEngine
        emulator = Emulator(theory.pt, engine=TaylorEmulatorEngine(method='finite', order=4))
        emulator.set_samples()
        emulator.fit()
        emulator.save(emulator_fn)
    # likelihood.all_params gives access to the parameters of the likelihood pipeline
    if solve:
        for param in likelihood.all_params.select(name=['alpha*', 'sn*', 'c*']):
            if param.varied: param.update(derived='.auto')
        for param in likelihood.all_params.select(name=['al*']):
            if param.varied: param.update(prior=None, derived='.auto')
    if likelihood.mpicomm.rank == 0:
        likelihood.log_info('Use analytic marginalization for {}.'.format(likelihood.all_params.names(solved=True)))

    return likelihood


@tm_sample.python_app
def emulate(data, get_observable_likelihood=get_observable_likelihood, **kwargs):
    get_observable_likelihood(data=sum(dd.get().read().normalize() for dd in data), save_emulator=True, **kwargs)


@tm_profile.python_app
def profile(data, output, get_observable_likelihood=get_observable_likelihood, **kwargs):
    from desilike.profilers import MinuitProfiler
    likelihood = get_observable_likelihood(data=sum(dd.get().read().normalize() for dd in data), save_emulator=False, **kwargs)
    profiler = MinuitProfiler(likelihood, seed=42)
    profiles = profiler.maximize(niterations=10)
    output.write(profiles.save)


@tm_sample.python_app
def sample(data, output, get_observable_likelihood=get_observable_likelihood, resume=False, **kwargs):
    from desilike.samplers import EmceeSampler
    likelihood = get_observable_likelihood(data=sum(dd.get().read().normalize() for dd in data), save_emulator=False, **kwargs)
    save_fn = output.filepaths
    chains = None
    if resume: chains = save_fn
    sampler = EmceeSampler(likelihood, chains=chains, nwalkers=40, seed=42, save_fn=save_fn)
    sampler.run(min_iterations=2000, check={'max_eigen_gr': 0.03})


def cut_matrix(cov, xcov, ellscov, xlim):
    """
    The function cuts a matrix based on specified indices and returns the resulting submatrix.

    Parameters
    ----------
    cov : 2D array
        A square matrix representing the covariance matrix.

    xcov : 1D array
        x-coordinates in the covariance matrix.

    ellscov : list
        Multipoles in the covariance matrix.

    xlim : tuple
        `xlim` is a dictionary where the keys are `ell` and the values are tuples of two floats
        representing the lower and upper limits of `xcov` for that `ell` value to be returned.

    Returns
    -------
    cov : array
        Subset of the input matrix `cov`, based on `xlim`.
        The subset is determined by selecting rows and columns of `cov` corresponding to the
        values of `ell` and `xcov` that fall within the specified `xlim` range.
    """
    import numpy as np
    assert len(cov) == len(xcov) * len(ellscov), 'Input matrix has size {}, different than {} x {}'.format(len(cov), len(xcov), len(ellscov))
    indices = []
    for ell, xlim in xlim.items():
        index = ellscov.index(ell) * len(xcov) + np.arange(len(xcov))
        index = index[(xcov >= xlim[0]) & (xcov <= xlim[1])]
        indices.append(index)
    indices = np.concatenate(indices, axis=0)
    return cov[np.ix_(indices, indices)]


if __name__ == '__main__':

    fm = FileManager('files/y1_data.yaml', environ=environ)

    for tracer in ['LRG']:
        for zrange in [(0.4, 1.1)]:
            fmt = fm.select(tracer=tracer)
            data = []
            version = 'v0.4'
            for fi in fmt.select(filetype=['catalog', 'correlation'], version=version):  # iterate on NGC / SGC
                file = fi.get(filetype='correlation', zrange=zrange)
                file = compute_correlation(fi.get(keywords='catalog data'), fi.select(keywords='catalog randoms'), file)
                data.append(file)

            emulator_fn = '_emulators/emulator_{tracer}_{zrange[0]:.1f}_{zrange[1]:.1f}.npy'.format(tracer=tracer, zrange=zrange)
            fmz = fmt.select(zrange=zrange)
            covariance = fmz.get(keywords='covariance correlation', region='GCcomb', version='v0.1').filepath
            kwargs = dict(covariance=covariance, wmatrix=None,
                          theory_name='velocileptors', ells=(0, 2, 4),
                          template_name='shapefit', observable_name='corr',
                          emulator_fn=emulator_fn)

            emulate(data, **kwargs)
            profile(data, fmz.get(keywords='profile', region='GCcomb', version=version), **kwargs)
            sample(data, fmz.select(keywords='chain', region='GCcomb', version=version), **kwargs)