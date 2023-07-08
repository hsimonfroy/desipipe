from desipipe import Queue, Environment, TaskManager, FileManager, setup_logging


setup_logging()
queue = Queue('abacus_first_gen_hod')
environ = Environment('nersc-cosmodesi')

tm = TaskManager(queue=queue, environ=environ)

tm_power = tm.clone(scheduler=dict(max_workers=10), provider=dict(provider='nersc', mpiprocs_per_worker=64))
tm_profile = tm.clone(scheduler=dict(max_workers=10), provider=dict(provider='nersc', mpiprocs_per_worker=6, nodes_per_worker=0.2))
tm_sample = tm.clone(scheduler=dict(max_workers=10), provider=dict(provider='nersc', mpiprocs_per_worker=32, nodes_per_worker=0.5))


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


def get_observable_likelihood(data=None, covariance=None, wmatrix=None, theory_name='velocileptors', ells=(0, 2, 4), template_name='shapefit', observable_name='power', tracer='LRG',
                              solve=True, save_emulator=False, emulator_fn=None):

    """Return the power spectrum likelihood, optionally computing the emulator (if ``save_emulator``)."""

    from desilike.observables.galaxy_clustering import TracerPowerSpectrumMultipolesObservable, TracerCorrelationFunctionMultipolesObservable
    from desilike.likelihoods import ObservablesGaussianLikelihood

    z, b0, klim, slim = get_fit_setup(tracer, ells=ells, theory_name=theory_name)
    z = data[0].attrs['z']

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
        observable = TracerPowerSpectrumMultipolesObservable(klim=klim, data=data, covariance=covariance, wmatrix=wmatrix, kinlim=(0., 0.25), theory=theory)

    if observable_name == 'corr':
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


@tm_power.python_app
def compute_power(data, output):
    import numpy as np
    from mockfactory import utils
    from pypower import CatalogFFTPower

    z = float(data.options['z'])
    data = data.read()
    positions = np.column_stack([data[name] for name in ['x', 'y', 'z']])
    velocities = np.column_stack([data[name] for name in ['vx', 'vy', 'vz']])
    from cosmoprimo.fiducial import DESI
    cosmo = DESI()
    a = 1. / (1. + z)
    E = cosmo.efunc(z)
    los = output.options['z']
    los = [1. * (los == axis) for axis in 'xyz']
    positions += utils.vector_projection(velocities / (100. * a * E), los)
    power = CatalogFFTPower(data_positions1=positions, position_type='pos',
                            boxsize=2000., boxcenter=1000., nmesh=512, resampler='tsc', interlacing=3, wrap=True)
    power.attrs['z'] = z
    output.save(power)
    return output


@tm_power.python_app
def compute_window(power, output):
    import numpy as np
    from pypower import MeshFFTWindow
    power = power.read()
    edgesin = np.linspace(0., 0.5, 1001)
    window = MeshFFTWindow(edgesin=edgesin, power_ref=power, periodic=True)
    output.save(window.save)


@tm_profile.python_app
def emulate(data, get_observable_likelihood=get_observable_likelihood, **kwargs):
    get_observable_likelihood(data=[dd.get().read() for dd in data], save_emulator=True, **kwargs)


@tm_profile.python_app
def profile(data, output, get_observable_likelihood=get_observable_likelihood, **kwargs):
    from desilike.profilers import MinuitProfiler
    likelihood = get_observable_likelihood(data=[dd.get().read() for dd in data], save_emulator=False, **kwargs)
    profiler = MinuitProfiler(likelihood, seed=42)
    profiles = profiler.maximize(niterations=10)
    output.write(profiles.save)


@tm_sample.python_app
def sample(data, output, get_observable_likelihood=get_observable_likelihood, resume=False, **kwargs):
    from desilike.samplers import EmceeSampler
    likelihood = get_observable_likelihood(data=[dd.get().read() for dd in data], save_emulator=False, **kwargs)
    save_fn = output.filepaths
    chains = None
    if resume: chains = save_fn
    sampler = EmceeSampler(likelihood, chains=chains, nwalkers=40, seed=42, save_fn=save_fn)
    sampler.run(min_iterations=2000, check={'max_eigen_gr': 0.03})


if __name__ == '__main__':

    fm = FileManager('data/y1_first_gen.yaml', environ=environ)
    tracer = 'LRG'
    fm_abacus = fm.select(keywords='abacus box alternative hod', tracer=tracer)
    data = {}
    for ihod in range(1, 9):
        data[ihod] = []
        for fi in fm_abacus.select(filetype=['catalog', 'power'], ihod=ihod):
            for los in ['x', 'y', 'z']:
                file = fi.get(filetype='power', los=los)
                file = compute_power(fi.get(filetype='catalog'), file)
                data[ihod].append(file)
    compute_window(data[1], fm.get(keywords='box 2gpc window'))

    fm_ez = fm.select(keywords=['y1 ez box'], tracer=tracer)
    emulator_fn = '_emulators/emulator.npy'
    kwargs = dict(covariance=fm_ez.filepaths, wmatrix=fm.get(keywords='window box abacus').filepath,
                  theory_name='velocileptors', ells=(0, 2, 4),
                  template_name='shapefit', observable_name='power',
                  tracer=tracer, emulator_fn=emulator_fn)
    emulate(data[1], **kwargs)
    for ihod, data in data.items():
        profile(data, fm_abacus.get(keywords='profile', ihod=ihod), **kwargs)
        sample(data, fm_abacus.select(keywords='chain', ihod=ihod), **kwargs)
