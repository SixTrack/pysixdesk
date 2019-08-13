from .utils import merge_dicts
# LHC settings
LHC = {}
# TODO: fill these in.
# defaults
_LHC_DEF = dict(phi_IR1=90.000,
                phi_IR5=0.000,
                b_t_dist=25.,
                # bunch_charge=2.2e11,
                # emit_norm=2.5e-6,
                inttunex=62.0,
                inttuney=60.0,
                qprimex=3.0,
                qprimey=3.0,
                fraction_crab=0,
                SEEDRAN=1,
                lhcbeam=1  # 2: b2 clockwise, 4: b2 counter clockewise
                )
# injection defaults
_LHC_INJ = dict(rfvol=8.0,  # [MV]
                sigz=0.11,  # [mm]
                sige=4.5e-04,
                e0=450.0,
                # I_MO= ,
                tunex=62.28,
                tuney=60.31,
                )
# collision defaults
_LHC_COL = dict(rfvol=16.0,  # [MV]
                sigz=0.77e-1,  # [mm]
                sige=1.1e-04,
                e0=7000.0,
                # I_MO= ,
                tunex=62.31,
                tuney=60.32,
                )
LHC['inj'] = merge_dicts(_LHC_DEF, _LHC_INJ)
LHC['col'] = merge_dicts(_LHC_DEF, _LHC_COL)

# HLLHC settings
HLLHC = {}
# defaults
_HLLHC_DEF = dict(phi_IR1=90.000,
                  phi_IR5=0.000,
                  b_t_dist=25.,
                  bunch_charge=2.2e11,
                  emit_norm=2.5,
                  inttunex=62.0,
                  inttuney=60.0,
                  qprimex=3.0,
                  qprimey=3.0,
                  fraction_crab=0,
                  SEEDRAN=1,
                  lhcbeam=1  # 2: b2 clockwise, 4: b2 counter clockewise
                  )
# injection defaults
_HLLHC_INJ = dict(rfvol=8.0,  # [MV]
                  sigz=0.130,  # [mm]
                  sige=4.5e-04,
                  e0=450.0,
                  I_MO=-20,
                  tunex=62.28,
                  tuney=60.31,
                  )
# collision defaults
_HLLHC_COL = dict(rfvol=8.0,  # [MV]
                  sigz=0.075,  # [mm]
                  sige=1.1e-4,
                  e0=7000.0,
                  I_MO=-570,
                  tunex=62.31,
                  tuney=60.32,
                  )
HLLHC['inj'] = merge_dicts(_HLLHC_DEF, _HLLHC_INJ)
HLLHC['col'] = merge_dicts(_HLLHC_DEF, _HLLHC_COL)
